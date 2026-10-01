# core/process_service.py
# -*- coding: utf-8 -*-
"""공정 흐름(시작 → CSV 스텝 진행 → 종료·중단) — Qt·화면·장치를 모르는 서비스.

리팩토링 3단계에서 MainDialog 의 공정 흐름을 세 번에 나눠 이리로 옮긴다.
  3a: 공정 시작·레시피 적재   3b: CSV 스텝·딜레이·리스트 취소   3c: STOP·ALL STOP·종료·오류·재기동·설비 이상 중단
장치 동작(PLC 비상정지·DC 비상 OFF·RF Pulse 정지·히터 레시피 정지·공정 정지 신호)은 한 줄짜리 port 다.
Qt 객체(딜레이 QTimer·QElapsedTimer)는 MainDialog 에 남고, 서비스는 ports 로 타이머 시작/정지·경과 시간만 부른다.
바깥 일(경고창, 로그, 단계 표시, 버튼, 챗, ERP, 기록 파일, 장치 신호, 히터 확인)은 전부 ports 로만 부른다.
ports 는 main.py 가 구현한다. 본문의 순서·조건·문구는 옮기기 전 main.py 그대로다.

이 모듈은 PyQt6·UI·main·controller·device·reporter·lib.logger·lib.heater_logger·lib.recipe_io 를 import 하지 않는다.
"""
import os
from pathlib import Path
from typing import Iterable, Optional, Protocol, Sequence

from core.params import ManualInputs, build_manual_params
from core.recipe import csv_rows_use_heater, fmt_hms, parse_delay_seconds, row_has_content
from core.state import ProcessState


class ProcessPorts(Protocol):
    """ProcessService 가 부르는 바깥 일. main.py 의 _MainProcessPorts 가 구현한다."""

    def alert(self, kind: str, title: str, text: str) -> None:
        """경고창(kind: warning/critical/information). 원격 명령 중이면 창 대신 실패 사유로 모인다."""

    def log(self, level: str, msg: str) -> None:
        """로그 한 줄(로그창·공정 로그 파일)."""

    def stage(self, text: str) -> None:
        """단계 표시(stage monitor) 문구를 바꾼다."""

    def set_buttons(self, start: bool, stop: bool, select_csv: Optional[bool] = None) -> None:
        """Start → Stop → 레시피 선택 버튼 순서로 활성 상태를 정한다(select_csv 가 None 이면 그대로)."""

    def is_closing(self) -> bool:
        """프로그램 종료가 시작됐는가."""

    def read_manual_inputs(self) -> ManualInputs:
        """수동 입력칸 원시값을 읽는다."""

    def show_manual_inputs(self, inputs: ManualInputs) -> None:
        """입력값을 노트북 수동 입력칸에 보여 준다(원격 수동 시작이 통과한 뒤 — 체크·값 칸, offset·param 포함)."""

    def apply_params_to_ui(self, params: dict) -> None:
        """공정 params 를 입력칸에 보여 준다(CSV 미리보기)."""

    def build_csv_params(self, row: dict) -> dict:
        """CSV 한 행 → 공정 params(화면 offset/param 칸 포함). 잘못되면 ValueError."""

    def open_process_log(self, prefix: str) -> None:
        """이번 공정용 로그 파일을 연다."""

    def reset_stats(self) -> None:
        """ChK_log 평균값 누적을 새로 시작한다."""

    def load_table(self, path: str, preferred_sheets: Sequence[str]) -> Iterable[dict]:
        """레시피 파일(CSV/TSV/XLSX)을 행 목록으로 읽는다."""

    def command_origin(self) -> str:
        """지금 요청을 누가 했는가 — 원격 명령 처리 중이면 "erp", 아니면 "local"."""

    def main_valve_open(self) -> bool:
        """메인밸브·인터락이 열려 있는가(아니면 경고창을 띄우고 False)."""

    def clear_plc_fault(self) -> None:
        """PLC 통신 실패 래치를 푼다."""

    def request_start(self, params: dict) -> None:
        """공정 컨트롤러에 시작을 요청한다(params)."""

    def heater_recipe_running(self) -> bool:
        """히터 레시피가 돌고 있는가(히터 사용 설정이 꺼져 있으면 False)."""

    def heater_gas_guard(self) -> bool:
        """히터 가스·압력이 MFC 를 쥐고 있지 않은가(쥐고 있으면 경고창을 띄우고 False)."""

    def log_heater_header(self, params: dict) -> None:
        """공정 로그 머리말에 히터 설정 한 줄을 남긴다."""

    def chat_reset_run_state(self) -> None:
        """챗·ERP 의 공정 1회분 상태를 새로 시작한다."""

    def chat_notify_started(self, params: dict, name: str) -> None:
        """구글챗 공정 시작 카드."""

    def erp_run_start(self, name: str, params: dict) -> None:
        """ERP 에 공정 시작을 보고한다."""

    def chat_enabled(self) -> bool:
        """구글챗 알림을 쓰는가(웹훅이 설정돼 있는가)."""

    def chat_text(self, msg: str) -> None:
        """구글챗 일반 메시지 한 줄(보내고 바로 flush)."""

    def chat_notify_failed_now(self, reason: str, send_text: bool = False) -> None:
        """이번 공정의 실패 원인을 저장한다(send_text=True 면 일반챗도 바로 보낸다)."""

    def chat_add_error(self, text: str) -> None:
        """종료 카드에 실을 오류 한 줄을 더한다."""

    def chat_notify_finished(self, ok: bool) -> None:
        """공정 종료 보고 — ERP run_end 1회 + 구글챗 종료 카드(+실패면 원인 한 줄)."""

    def chat_user_stopped(self) -> bool:
        """이번 공정을 사용자가 STOP 했는가."""

    def notice(self, source: str, kind: str, title: str, text: str) -> None:
        """장비가 스스로 내는 알림(ERP 알림 + 노트북 출처면 경고창)."""

    def reset_process_ui_fields(self) -> None:
        """공정 입력칸·계측 표시를 초기 상태로 되돌린다."""

    def close_process_log(self) -> None:
        """이번 공정 로그 파일을 닫는다(이후 로그는 기본 log.txt)."""

    def delay_timer_start(self) -> None:
        """CSV 딜레이 1초 틱 타이머를 새로 만들어 시작한다(틱마다 on_delay_tick)."""

    def delay_timer_stop(self) -> None:
        """CSV 딜레이 타이머를 멈추고 치운다(시계도 함께 지운다)."""

    def delay_clock_start(self) -> None:
        """CSV 딜레이 경과 시간 시계를 새로 시작한다."""

    def delay_elapsed_ms(self) -> Optional[int]:
        """CSV 딜레이 시계의 경과 ms. 시계가 없으면 None."""

    def delay_clock_clear(self) -> None:
        """CSV 딜레이 시계를 지운다."""

    def status_message(self, level: str, msg: str) -> None:
        """장치·공정 상태 메시지 한 줄(로그를 남기고, level 이 "재시작"이면 재시작 처리까지 — 창의 on_status_message)."""

    def mark_user_stopped(self) -> None:
        """이번 공정을 사용자 STOP 으로 표시한다(종료 카드 "정지")."""

    def mark_emergency_stopped(self) -> None:
        """이번 공정을 ALL STOP(비상 정지)으로 표시한다(종료 카드 "긴급 중단")."""

    def mark_fault_abort(self) -> None:
        """설비 이상 중단이 시작됐다고 표시한다."""

    def chat_fault_detail_sent(self) -> bool:
        """이번 공정의 설비 이상 상세 카드를 이미 보냈는가."""

    def mark_chat_fault_detail_sent(self) -> None:
        """설비 이상 상세 카드를 보냈다고 표시한다(보내기 전에 세운다)."""

    def chat_send_fault_detail(self, reason: str, detail: str) -> None:
        """설비 이상 중단 사유·상태를 구글챗으로 한 번 보낸다."""

    def process_controller_present(self) -> bool:
        """공정 컨트롤러가 있는가(정지 요청을 보낼 곳)."""

    def request_stop(self) -> None:
        """공정 컨트롤러에 정지를 요청한다(종료 시퀀스 → finished)."""

    def plc_emergency_stop(self) -> None:
        """PLC 비상정지(코일 전부 OFF)."""

    def dc_emergency_off(self) -> None:
        """DC 파워 직결 비상 OFF(PLC 를 거치지 않는다)."""

    def rfpulse_stop(self) -> None:
        """RF Pulse(CESAR) 직결 정지(PLC 를 거치지 않는다)."""

    def heater_recipe_stop(self, reason: str) -> None:
        """히터 레시피를 멈춘다(히터 OFF 포함)."""

    def clear_erp_meas(self) -> None:
        """ERP 로 보내던 MFC 실측값을 비운다."""

    def build_chk_csv_row(self) -> dict:
        """이번 공정의 ChK_log.csv 한 줄을 만든다(입력값 + 평균 계측값)."""

    def append_chk_csv_row(self, row: dict) -> bool:
        """ChK_log.csv 에 한 줄 쓴다. 성공하면 True."""


class ProcessService:
    def __init__(self, state: ProcessState, ports: ProcessPorts):
        self.st = state
        self.ports = ports

    # ==================== 공정 시작 ====================
    def recipe_display_name(self) -> str:
        """적재된 레시피의 표시 이름(파일 이름)."""
        return os.path.basename(str(self.st.csv_file_path or ""))

    def start(self, manual_inputs: Optional[ManualInputs] = None) -> None:
        """Start 버튼·원격 PROCESS_START·RECIPE_PROCESS_START 공통. 적재된 레시피가 있으면 CSV 리스트, 없으면 수동 공정.
        manual_inputs 가 있으면 원격 수동 시작이다 — 노트북 입력칸 대신 그 값으로 시작하고, 레시피가 적재돼 있으면 거부한다."""
        st, ports = self.st, self.ports
        # 원격 수동 시작만: 공정 중이거나 레시피가 적재돼 있으면 거부(노트북 입력칸은 건드리지 않는다)
        if manual_inputs is not None:
            if st.is_active():
                ports.alert("warning", "경고", "이미 공정이 진행 중입니다.")
                return
            if st.csv_file_path:
                ports.alert("warning", "시작 불가",
                            f"장비에 레시피가 적재돼 있습니다({self.recipe_display_name()}). "
                            "레시피로 시작하거나 적재를 해제하세요.")
                return
        # 히터 레시피가 돌아도, 이 공정이 히터를 건드리지 않으면 시작을 허용한다.
        if ports.heater_recipe_running():
            if st.csv_file_path and csv_rows_use_heater(st.csv_rows):
                ports.alert("warning", "시작 불가",
                            "히터 레시피가 실행 중입니다.\n"
                            "이 공정 레시피에는 히터 목표값이 들어 있어 함께 실행할 수 없습니다.\n"
                            "히터 레시피를 중단하거나, 레시피에서 히터값을 빼세요.")
                return
            # 수동 모드는 아래에서 use_heater 를 강제로 끈다
        if st.running:
            ports.alert("warning", "경고", "이미 공정이 진행 중입니다.")
            return

        # 히터 전용 가스·압력이 MFC 를 쥐고 있으면 공정이 그 위에 겹칠 수 없다
        if not ports.heater_gas_guard():
            return

        # ★ 메인밸브 개방 확인: MV & MV_INTERLOCK 둘 다 ON일 때만 공정 시작 허용
        if not ports.main_valve_open():
            return

        # 출처 = 이 공정을 시작한 쪽(가드를 모두 통과한 지점). CSV 리스트의 뒤 스텝도 이 값을 쓴다.
        st.origin = ports.command_origin()

        ports.clear_plc_fault()

        # === 1) CSV 모드인지 먼저 확인 ===
        if st.csv_file_path:
            # CSV 로딩 & 리스트 공정 모드 진입
            if not self.load_csv_list():
                return  # 로딩 실패
            st.csv_mode = True
            self.start_next_csv_step()
            return

        try:
            params = build_manual_params(manual_inputs if manual_inputs is not None
                                         else ports.read_manual_inputs())

        except (ValueError, TypeError) as e:
            ports.alert("warning", "입력 오류", f"공정 파라미터가 잘못되었습니다:\n{e}")
            return

        # 원격 수동 시작이 통과했다 — 이제 노트북 입력칸에도 보여 준다
        if manual_inputs is not None:
            ports.show_manual_inputs(manual_inputs)

        # ★ 단일 공정(수동 Start)일 때의 공정 이름
        st.current_name = "Single CHK"

        # ★ 수동 공정도 CSV 공정과 동일한 로그 포맷을 위해
        #    이번 공정 파라미터를 저장 + 평균값 누적 초기화
        st.last_params = dict(params)
        ports.reset_stats()
        st.step_ok = True   # 이번 공정은 정상 종료로 가정하고 시작

        # ★★★ 여기서 이번 공정용 로그 파일을 NAS에 생성 (CHK_YYYYmmdd_HHMMSS.txt) ★★★
        ports.open_process_log("CHK")
        ports.log("정보", "=== CHK 공정 시작 ===")

        # 이번 공정이 히터를 소유하는가 (히터 레시피와의 충돌 판정 기준)
        st.heater_claimed = bool(
            params.get("use_heater") and float(params.get("heater_temp") or 0) > 0)
        if ports.heater_recipe_running() and not st.heater_claimed:
            ports.log(
                "정보",
                "[히터] 히터 레시피가 제어 중입니다. 이번 공정은 히터를 제어하지 않습니다.")

        # 이번 공정의 히터 설정을 로그 머리말에 남긴다
        ports.log_heater_header(params)

        ports.chat_reset_run_state()
        ports.chat_notify_started(params, st.current_name)

        try:
            ports.erp_run_start(
                st.current_name or params.get("process_note", "") or "CHK 공정",
                params,
            )
        except Exception:
            pass

        ports.request_start(params)

        st.running = True
        ports.set_buttons(False, True, False)

    # ==================== 레시피 적재 ====================
    def load_recipe_file(self, path: str) -> None:
        """파일 대화상자 없이 지정된 CSV/엑셀 레시피를 적재한다(원격 실행용).

        _on_select_csv_clicked 가 경로를 얻은 뒤 수행하는 처리와 동일하다.
        (UI 경로와 동작을 하나로 유지하기 위해 본문을 이쪽으로 옮겼다)
        """
        st, ports = self.st, self.ports
        if ports.is_closing():
            return

        # UI 경로는 대화상자 앞에서 이미 막지만, 원격 호출은 여기서 막아야 한다
        if st.is_active():
            ports.alert("warning",
                        "변경 불가",
                        "공정 진행 중에는 CSV 파일을 변경할 수 없습니다."
                        )
            return

        if not path:
            return

        p = Path(path)
        if not p.exists():
            ports.alert("warning", "파일 오류", "선택한 CSV 파일을 찾을 수 없습니다.")
            return

        st.csv_file_path = str(p)
        ports.log("정보", f"CSV 공정 리스트 파일 선택: {p}")

        if not self.load_csv_list():
            return

        if ports.is_closing() or not st.csv_rows:
            return

        first_row = st.csv_rows[0]
        first_name = (first_row.get("Process_name") or "").strip()
        delay_sec = parse_delay_seconds(first_name)

        if delay_sec is not None:
            ports.stage(f"CSV 공정: 1/{len(st.csv_rows)} - {first_name} (대기 스텝)")
            return

        try:
            params = ports.build_csv_params(first_row)
        except Exception as e:
            ports.alert("warning", "CSV 레시피 오류", f"첫 번째 공정 파라미터가 잘못되었습니다:\n{e}")
            ports.stage(f"CSV 공정: 1/{len(st.csv_rows)} - (오류)")
            return

        if ports.is_closing():
            return

        ports.apply_params_to_ui(params)
        name = params.get("process_name") or "STEP 1"
        ports.stage(f"CSV 공정: 1/{len(st.csv_rows)} - {name}")

    def load_csv_list(self) -> bool:
        """
        st.csv_file_path 에 지정된 CSV를 읽어서
        st.csv_rows 에 List[dict] 형태로 저장.
        성공하면 True, 실패하면 False.
        """
        st, ports = self.st, self.ports
        if not st.csv_file_path:
            ports.alert("warning", "CSV 없음", "먼저 CSV 파일을 선택해 주세요.")
            return False

        try:
            # 입력 소스만 바뀐다 — CSV/TSV/XLSX 를 같은 list[dict] 로 받는다.
            reader = ports.load_table(
                st.csv_file_path,
                ("Recipe", "recipe", "공정", "Sheet1"))

            rows = [row for row in reader if row_has_content(row)]

        except Exception as ex:
            ports.alert("critical", "CSV 읽기 오류", f"CSV 파일을 읽는 중 오류가 발생했습니다.\n\n{ex}")
            return False

        if not rows:
            ports.alert("warning", "CSV 비어있음", "CSV 파일에 유효한 공정 행이 없습니다.")
            return False

        st.csv_rows = rows
        st.csv_index = -1
        return True

    # ==================== CSV 리스트 진행 ====================
    def start_next_csv_step(self) -> None:
        """csv_rows[csv_index+1] 공정을 하나 실행하거나, 모두 끝났으면 CSV 모드 종료."""
        st, ports = self.st, self.ports
        # ✅ STOP 등으로 CSV 리스트 전체가 취소된 뒤에
        #    _start_next_csv_step 이 호출되면 아무 것도 하지 않고 무시
        if not st.csv_mode or not st.csv_rows or st.csv_cancelled:
            ports.log(
                "정보",
                "_start_next_csv_step 호출됐지만 CSV 모드가 아니거나 취소 플래그가 켜져 있어서 무시합니다.",
            )
            return

        st.csv_index += 1

        # 모든 행을 다 돌았으면 종료
        if st.csv_index >= len(st.csv_rows):
            st.clear_csv_list()     # 이번 CSV 회차 공정 이름/파라미터/파일 선택 흔적도 함께 제거
            st.running = False

            # ✅ UI도 대기 상태로 정리
            ports.set_buttons(True, False, True)
            ports.stage("CSV 공정 완료")

            # ▶ 공통 UI 초기화
            ports.reset_process_ui_fields()
            # 리스트 정상 완료 — 이후 로그가 마지막 STEP 파일에 덧붙지 않게 해제
            ports.close_process_log()
            ports.notice("process", "information", "CSV 공정 완료", "CSV에 있는 모든 공정을 완료했습니다.")
            return

        row = st.csv_rows[st.csv_index]

        # ✅ 여기서 step_no/total 먼저 고정(딜레이 포함 모든 분기에서 동일하게 쓰기)
        step_no = st.csv_index + 1
        total   = len(st.csv_rows)

        raw_name = (row.get("Process_name") or "").strip()
        delay_sec = parse_delay_seconds(raw_name)
        if delay_sec is not None:
            if delay_sec <= 0:
                ports.log("정보", f"CSV DELAY 스킵: {raw_name} (0초)")
                self.start_next_csv_step()
                return

            # ✅ 딜레이도 "현재 공정명"을 CSV 1/3 형태로 잡아두면 STOP/실패 텍스트가 안 헷갈림
            st.current_name = f"CSV {step_no}/{total} - {raw_name}"

            self.start_delay_step(delay_sec, raw_name)
            return

        # (안전) '#'(번호)만 채워진 빈 행은 그냥 스킵
        if not row_has_content(row):
            ports.log("정보", f"CSV 빈 행 스킵: index={st.csv_index + 1}")
            self.start_next_csv_step()
            return

        try:
            params = ports.build_csv_params(row)
        except Exception as e:
            _row_msg = f"CSV {st.csv_index + 1}번째 행 파라미터가 잘못되었습니다:\n{e}"
            ports.log("ERROR", f"CSV 레시피 오류로 리스트 공정을 중단합니다: {e}")

            # ✅ 구글챗에도 실패 알림(종료 카드 + 일반챗 1줄) 보장
            st.step_ok = False
            try:
                # 이 케이스는 '공정 started'를 안 보냈을 수도 있으니, 상태를 새로 잡아줌
                ports.chat_reset_run_state()

                # 위에서 step_no/total을 이미 만든 상태(네 코드 기준)라서 그대로 사용
                reason = f"CSV 레시피 오류: {e}"
                st.current_name = f"CSV {step_no}/{total} - (레시피 오류)"
                ports.chat_notify_failed_now(reason, send_text=False)
                ports.chat_notify_finished(False)
            except Exception:
                pass

            # ✅ 여기서는 중복 전송 방지 위해 notify_chat=False
            self.cancel_csv_list_now("CSV 레시피 오류로 중단", notify_chat=False)
            ports.notice("process", "critical", "CSV 레시피 오류", _row_msg)   # 정리 뒤에 알린다
            return

        # ★ 이번 CSV STEP도 수동 공정과 동일한 로그 포맷을 위해
        #    파라미터 저장 + 평균값 누적 초기화
        st.last_params = dict(params)
        ports.reset_stats()
        st.step_ok = True   # 이 STEP이 정상 종료했을 때만 CSV에 기록

        # ✅ 이 단계의 파라미터를 UI에 반영 (CH1/CH2처럼 보이게)
        ports.apply_params_to_ui(params)

        # ★★★ 이 CSV STEP 전용 로그 파일 생성 ★★★
        ports.open_process_log("CHK")

        # 이번 STEP 이 히터를 소유하는가 (히터 레시피와의 충돌 판정 기준)
        st.heater_claimed = bool(
            params.get("use_heater") and float(params.get("heater_temp") or 0) > 0)
        if ports.heater_recipe_running() and not st.heater_claimed:
            ports.log(
                "정보",
                "[히터] 히터 레시피가 제어 중입니다. 이번 공정은 히터를 제어하지 않습니다.")

        # 이번 공정의 히터 설정을 로그 머리말에 남긴다
        ports.log_heater_header(params)

        # ✅ (위에서 step_no/total을 이미 만들었다면 여기 재정의 필요 없음)
        # step_no = self.csv_index + 1
        # total   = len(self.csv_rows)

        ports.log("정보", f"=== CHK CSV STEP {step_no}/{total} 시작 ===")

        # 로그/스테이지 표시
        base_name = (
            params.get("process_name")
            or params.get("Process_name")
            or f"STEP {step_no}/{total}"
        )

        # ✅ 이 STEP의 표시명(=CSV 1/3 포함)으로 통일
        display = f"CSV {step_no}/{total} - {base_name}"

        # ✅ 공정 종료 카드/실패 텍스트가 이 값을 보게 됨 → CSV 1/3이 항상 유지됨
        st.current_name = display

        ports.chat_reset_run_state()
        ports.chat_notify_started(params, display)

        ports.log("Process", f"CSV 공정 리스트 {step_no}/{total} 실행: {display}")
        ports.stage(display)

        # 실제 공정 시작
        st.running = True
        ports.set_buttons(False, True, False)

        ports.clear_plc_fault()              # ✅ 추가: 스텝 시작마다 PLC 실패 래치 초기화

        try:
            ports.erp_run_start(
                st.current_name or params.get("process_note", "") or "CHK 공정",
                params,
            )
        except Exception:
            pass

        ports.request_start(params)

    # ==================== CSV Delay (공정 사이 대기) ====================
    def start_delay_step(self, delay_sec: int, raw_name: str) -> None:
        """CSV 리스트 중 'delay Xm' 스텝 실행: UI는 멈추지 않고(타이머로) 카운트다운."""
        st, ports = self.st, self.ports
        ports.delay_timer_stop()

        st.delay_active = True
        st.delay_total_sec = max(int(delay_sec), 0)
        st.delay_remaining_sec = st.delay_total_sec
        st.delay_name = raw_name

        # ✅ 시작시간(모노토닉) 기록
        ports.delay_clock_start()

        step_no = st.csv_index + 1
        total = len(st.csv_rows)

        # 공정처럼 보이게 UI 버튼 상태 유지
        st.running = True
        ports.set_buttons(False, True)

        ports.log(
            "Process",
            f"CSV DELAY STEP {step_no}/{total} 시작: {raw_name} (총 {fmt_hms(st.delay_total_sec)})",
        )

        # 즉시 1회 표시 (UI에 알아보기 쉽게)
        ports.stage(
            f"CSV {step_no}/{total} - {raw_name} (남은 {fmt_hms(st.delay_remaining_sec)})"
        )

        # ✅ 구글챗(딜레이 시작 1회)
        try:
            if ports.chat_enabled():
                name = st.current_name or f"CSV {step_no}/{total} - {raw_name}"
                ports.chat_text(
                    f"⏳ CHK 딜레이 시작: {name} | 총 {fmt_hms(st.delay_total_sec)}"
                )
        except Exception:
            pass

        # 1초마다 카운트다운
        ports.delay_timer_start()

    def on_delay_tick(self) -> None:
        """CSV 딜레이 1초 틱 — 남은 시간 표시, 다 되면 다음 스텝."""
        st, ports = self.st, self.ports
        # 외부에서 취소/종료된 경우
        if (not st.csv_mode) or st.csv_cancelled or (not st.delay_active):
            ports.delay_timer_stop()
            return

        # ✅ elapsed 기반으로 남은 시간 재계산 (UI 렉이 있어도 누적오차 없음)
        elapsed_ms = ports.delay_elapsed_ms()
        if elapsed_ms is not None:
            elapsed_sec = int(elapsed_ms // 1000)
            st.delay_remaining_sec = max(st.delay_total_sec - elapsed_sec, 0)
        else:
            # 혹시 모를 fallback
            st.delay_remaining_sec = max(st.delay_remaining_sec - 1, 0)

        step_no = st.csv_index + 1
        total = len(st.csv_rows)

        if st.delay_remaining_sec <= 0:
            ports.delay_timer_stop()
            st.delay_active = False
            st.running = False
            ports.log("정보", f"CSV DELAY 완료: {st.delay_name}")

            # ✅ 구글챗(딜레이 완료 1회)
            try:
                if ports.chat_enabled():
                    name = st.current_name or f"CSV {step_no}/{total} - {st.delay_name}"
                    ports.chat_text(f"✅ CHK 딜레이 완료: {name}")
            except Exception:
                pass

            self.start_next_csv_step()
            return

        ports.stage(
            f"CSV {step_no}/{total} - {st.delay_name} (남은 {fmt_hms(st.delay_remaining_sec)})"
        )

    # ==================== CSV 리스트 즉시 정리 ====================
    def cancel_csv_list_now(
        self,
        stage_text: str = "CSV 공정 취소됨",
        *,
        notify_chat: bool = True,
        reason: Optional[str] = None,
    ) -> None:
        """CSV 리스트 공정을 즉시 정리(딜레이/스텝 사이/즉시 취소 등에서 공통 사용)."""
        st, ports = self.st, self.ports

        # ✅ (추가) finished 시그널을 안 거치는 케이스에서도 구글챗 종료/실패 알림 보장
        if notify_chat and ports.chat_enabled():
            try:
                # reason이 있으면 그걸 '실패 원인'으로 저장
                if reason:
                    ports.chat_notify_failed_now(reason, send_text=False)
                else:
                    # STOP이면 stage_text를 굳이 error로 넣지 않게(카드가 깔끔)
                    if not ports.chat_user_stopped():
                        ports.chat_add_error(stage_text)

                # 카드에 찍힐 공정명 보정
                if not (st.current_name or "").strip():
                    st.current_name = stage_text

                # 종료 카드 + (실패면) 일반챗 1줄(단, STOP이면 일반챗 추가 전송 안 함)
                ports.chat_notify_finished(False)
                # 이 공정의 종료 처리는 여기서 끝났다 — 뒤늦게 finished 가 와도 카드를 또 내지 않는다
                st.finish_handled = True
            except Exception:
                pass

        # === 기존 정리 로직 그대로 ===
        ports.delay_timer_stop()
        st.delay_active = False
        st.delay_total_sec = 0
        st.delay_remaining_sec = 0
        st.delay_name = ""
        ports.delay_clock_clear()

        st.csv_cancelled = False
        st.clear_csv_list()

        st.running = False
        ports.set_buttons(True, False, True)
        ports.stage(stage_text)
        ports.reset_process_ui_fields()
        # 반드시 맨 끝 — 중단 사유 로그는 해당 공정 파일에 남아야 한다
        ports.close_process_log()

    # ==================== STOP ====================
    def stop(self) -> None:
        """STOP 버튼 공통 처리: 현재 STEP 중단 + CSV 모드면 전체 리스트 취소."""
        st, ports = self.st, self.ports
        # STOP으로 중단된 공정은 정상 종료로 보지 않는다.
        st.step_ok = False
        ports.mark_user_stopped()
        ports.chat_add_error("사용자 STOP")
        ports.status_message("경고", "STOP 버튼 클릭됨")

        # ✅ CSV 리스트 공정 중이면, 이후 STEP들을 모두 취소하도록 플래그 설정
        if st.csv_mode:
            st.csv_cancelled = True
            ports.log("정보", "사용자 STOP → CSV 리스트 전체 취소 플래그 설정")

        # ✅ CSV Delay(대기) 중이면 즉시 타이머 끊고 리스트 정리
        if st.delay_active:
            ports.log("정보", "CSV Delay 중 STOP → 딜레이 즉시 중단 및 리스트 공정 취소")
            self.cancel_csv_list_now("CSV 공정 취소됨")   # ✅ 여기서 구글챗도 같이 보내게 됨(아래 _cancel_csv_list_now 수정)
            return

        # ✅ [추가] CSV 리스트인데 '스텝 사이'(process_running=False)라면 즉시 리스트 취소
        if st.csv_mode and (not st.running):
            ports.log("정보", "CSV STEP 사이 STOP → 리스트 공정 즉시 취소")
            self.cancel_csv_list_now("CSV 공정 취소됨")
            return

        # 실제 공정이 돌고 있으면 프로세스 스레드 쪽에 중단 요청
        if ports.process_controller_present() and st.running:
            ports.request_stop()

    # ==================== ALL STOP ====================
    def all_stop(self) -> None:
        """ALL STOP — 히터 레시피도 함께 멈춘다.

        레시피는 공정과 무관하게 돌 수 있으므로, 비상정지 뒤에도 레시피가
        살아 있으면 다음 스텝에서 히터를 다시 켜 버린다.
        (공정 STOP(정상 종료)에서는 레시피를 건드리지 않는다)
        """
        st, ports = self.st, self.ports
        # ALL STOP 은 어떤 예외에도 멈추지 않아야 한다 — 단계마다 따로 감싼다.
        try:
            if ports.heater_recipe_running():
                ports.heater_recipe_stop("비상 정지")
        except Exception:
            pass
        # 공정 상태머신도 세운다 — PLC 비상정지만으로는 컨트롤러가 다음 스텝을 계속 밟는다.
        #  ★ 비상정지로 끝난 공정은 '실패'로 기록되어야 한다. 그냥 request_process_stop 만
        #    보내면 _chk_process_ok 가 True 로 남아 구글챗에 "정상 종료" 카드가 나가고
        #    ChK_log.csv 에도 정상 공정으로 기록됐다. 사용자 STOP(_chat_user_stopped)과는
        #    구분해야 하므로 그 플래그는 건드리지 않고 별도 플래그로 "긴급 중단" 카드를 낸다.
        #  ★ 플래그는 request_plc_emergency_stop 보다 먼저 선다 — 비상정지로 코일이 전부 OFF 되면
        #    다음 폴링이 MV_button False 를 내는데, 그때 _mv_safety_armed() 가 이미 False 여야 이중 중단이 없다.
        _proc_active = bool(st.running)
        _csv_active = bool(st.csv_mode and st.csv_rows)
        _csv_delay = bool(st.delay_active)
        _handled_by_cancel = False
        if _proc_active or _csv_active or _csv_delay:
            try:
                st.step_ok = False
                ports.mark_emergency_stopped()
            except Exception:
                pass
        try:
            ports.plc_emergency_stop()
        except Exception:
            pass
        # DC 파워는 PLC 를 거치지 않는 직결 시리얼 — 비상정지로 안 꺼진다.
        #  공정 상태와 무관하게 직접 끈다(확인형). 공정 중이면 공정 정지 경로의 DC OFF 와 두 번 가지만 무해.
        try:
            ports.dc_emergency_off()
        except Exception:
            pass
        if _proc_active or _csv_active or _csv_delay:
            try:
                ports.chat_notify_failed_now("ALL STOP(비상 정지)으로 중단", send_text=False)
            except Exception:
                pass
            # CSV 리스트면 뒤 STEP 을 모두 막는다("재시작" 경로와 같은 처리)
            try:
                if _csv_active:
                    st.csv_cancelled = True
                    # 딜레이 스텝은 process_running=True 로 두지만 컨트롤러는 안 돈다.
                    #  여기서 리스트를 정리하면 종료 카드가 이미 나가므로, 뒤의
                    #  request_process_stop 은 보내지 않는다("재시작" 경로의 return 과 같다).
                    #  보내면 _stop_impl 의 '이미 정지' 분기가 finished 를 한 번 더 내서
                    #  카드가 2장 나간다.
                    if _csv_delay or not _proc_active:
                        self.cancel_csv_list_now("CSV 공정 취소", reason="ALL STOP(비상 정지)으로 중단")
                        _handled_by_cancel = True
            except Exception:
                pass
        # 실제 공정 스텝이 돌고 있을 때만 컨트롤러를 세운다. 안 돌고 있는데 보내면
        #  컨트롤러가 '이미 정지' 로 finished 를 내고 엉뚱한 종료 카드가 나간다.
        if _proc_active and not _handled_by_cancel:
            try:
                ports.request_stop()
            except Exception:
                pass
        # CESAR(RF Pulse)는 PLC 를 거치지 않는 직결 시리얼이라 비상정지로 안 꺼진다.
        #  공정이 안 돌고 있어도 펄스가 켜져 있을 수 있으니 드라이버를 직접 한 번 더 끈다.
        try:
            ports.rfpulse_stop()
        except Exception:
            pass

    # ==================== 설비 이상 중단 ====================
    def abort_by_fault(self, reason: str, detail: str = "") -> None:
        """설비 이상으로 공정을 즉시 중단한다(사용자 STOP 과 구분).

        온도가 무너진 시점에 그 시료는 이미 버린 것이라, 계속 돌리면
        타겟과 시간만 더 쓴다. 즉시 종료가 맞다.
        _chat_user_stopped 는 건드리지 않는다 — 그 플래그는 사용자 STOP
        전용이고, 설비 이상은 '실패'로 기록되어야 한다.
        """
        st, ports = self.st, self.ports
        try:
            if not st.is_active():
                return
        except Exception:
            return

        ports.mark_fault_abort()
        try:
            st.step_ok = False
        except Exception:
            pass
        try:
            ports.log("경고", f"[공정 중단] {reason}")
            if detail:
                for _l in str(detail).splitlines():
                    ports.log("경고", f"  {_l}")
        except Exception:
            pass
        try:
            ports.chat_add_error(reason)
            ports.chat_notify_failed_now(reason, send_text=False)
        except Exception:
            pass
        # 상세 카드는 공정 1회당 1장. 첫 사유가 대표 사유다.
        #  (중단 동작 자체는 아래에서 가드 없이 계속 수행된다)
        try:
            if not ports.chat_fault_detail_sent():
                ports.mark_chat_fault_detail_sent()     # 발송 전에 세운다
                ports.chat_send_fault_detail(reason, detail)
            else:
                ports.log(
                    "경고", f"[공정 중단] 추가 사유(챗 중복 발송 안 함): {reason}")
        except Exception:
            pass

        # CSV 리스트면 뒤 STEP 을 모두 취소한다
        try:
            if st.csv_mode:
                st.csv_cancelled = True
                ports.log("정보", "설비 이상 → CSV 리스트 전체 취소 플래그 설정")
            if st.delay_active:
                ports.log("정보", "CSV Delay 중 설비 이상 → 딜레이 중단 및 리스트 취소")
                self.cancel_csv_list_now("CSV 공정 중단됨(설비 이상)")
                return
            if st.csv_mode and (not st.running):
                self.cancel_csv_list_now("CSV 공정 중단됨(설비 이상)")
                return
        except Exception:
            pass

        try:
            if ports.process_controller_present() and st.running:
                ports.request_stop()
        except Exception:
            pass

    # ==================== 종료 처리 ====================
    def on_finished(self) -> None:
        """공정 컨트롤러 finished — 종료 보고·ChK_log 기록·CSV 다음 스텝/리스트 정리."""
        st, ports = self.st, self.ports
        # ★ 중복/헛호출 가드. 컨트롤러 _stop_impl 의 '이미 정지' 분기는 request_stop 을
        #   부르는 어느 경로에서든 finished 를 낼 수 있어, 호출부 하나를 막아도 다른
        #   경로에서 종료 카드 2장 / 엉뚱한 카드가 재발한다. 여기서 뿌리를 막는다.
        #   플래그는 _chat_reset_run_state(수동 시작 / CSV STEP 시작마다)에서 False 가 된다 —
        #   스텝 시작에서 리셋되지 않으면 2번째 STEP 부터 종료 처리가 통째로 사라지므로
        #   리셋 위치를 옮기지 말 것.
        if st.finish_handled:
            _alive = st.is_active()
            ports.log(
                "정보",
                "finished 무시 — 이번 공정의 종료 처리는 이미 끝났음"
                + ("" if _alive else " (시작된 공정 없음)"))
            return
        st.finish_handled = True

        ports.status_message("정보", "프로세스 종료중.")
        # 히터 소유권 해제 — 다음 공정/레시피 판정에 남지 않게 한다
        st.heater_claimed = False
        # MFC 폴링이 멈추므로 실측값도 함께 버린다(죽은 값 보고 방지)
        ports.clear_erp_meas()

        # ★ 이번 STEP이 정상 종료됐는지 여부를 먼저 보관
        last_step_ok = st.step_ok
        ports.chat_notify_finished(last_step_ok)

        # ★ 정상적으로 완료된 공정만 ChK_log.csv 에 한 줄 추가
        try:
            if last_step_ok:
                row = ports.build_chk_csv_row()
                ok = ports.append_chk_csv_row(row)
                if ok:
                    ports.log("정보", "ChK CSV 로그 저장 완료")
                else:
                    ports.log("경고", "ChK CSV 로그 저장 실패")
            else:
                ports.log("정보", "이번 공정은 비정상 종료 → ChK CSV에 기록하지 않음")
        except Exception as e:
            ports.log("경고", f"ChK CSV 로그 처리 중 예외 발생: {e!r}")
        finally:
            # 다음 공정을 위해 평균 누적값/상태 초기화
            ports.reset_stats()
            st.step_ok = False

        # ✅ 1) CSV 리스트 공정 모드인 경우
        if st.csv_mode and st.csv_rows:
            st.running = False

            # ✅ (1) 사용자가 STOP을 눌러 전체 리스트 취소한 경우
            if st.csv_cancelled:
                # 플래그 리셋
                st.csv_cancelled = False

                # CSV 상태 전체 초기화
                st.clear_csv_list()

                # ▶ 공정 상태 및 UI 초기화
                ports.set_buttons(True, False, True)
                ports.stage("CSV 공정 취소됨")
                ports.reset_process_ui_fields()
                ports.close_process_log()   # 리스트 종료 — 이후 로그는 log.txt 로
                return

            # ✅ (2) STOP이 아닌 경우: 성공/실패에 따라 분기
            if last_step_ok:
                # 이전 STEP이 정상 종료된 경우에만 다음 STEP으로 진행
                self.start_next_csv_step()
                return
            else:
                # ❌ 실패/에러로 끝난 경우 → 이후 STEP들은 실행하지 않고 CSV 리스트 공정 종료
                ports.log(
                    "경고",
                    "CSV 공정 중 실패 발생 → 다음 공정을 실행하지 않고 리스트 공정을 종료합니다.",
                )

                # CSV 상태 전체 초기화
                st.clear_csv_list()

                # 공정 상태 및 UI 초기화
                st.running = False
                ports.set_buttons(True, False, True)
                ports.stage("CSV 공정 실패로 중단됨")
                ports.reset_process_ui_fields()
                ports.close_process_log()   # 리스트 종료 — 이후 로그는 log.txt 로
                return

        # 🔻 여기 이하(단일 공정 종료 처리)는 그대로 유지
        st.running = False
        ports.set_buttons(True, False, True)
        ports.stage("공정 종료")

        # ▶ 공통 UI 초기화
        ports.reset_process_ui_fields()

        # 공정 로그 파일 해제 — 반드시 종료 처리의 맨 마지막.
        #  해제하지 않으면 공정이 끝난 뒤의 모든 로그(히터만 돌려도)가 이미 끝난
        #  공정의 .txt 에 계속 덧붙는다.
        #  CSV 리스트가 이어지는 중(_start_next_csv_step)에는 해제하지 않는다 —
        #  다음 STEP 이 set_process_log_file 로 새 파일을 만든다.
        ports.close_process_log()

    # ==================== 오류 ====================
    def on_critical_error(self, error_message: str) -> None:
        """공정 컨트롤러 critical_error — 실패 표시·원인 저장 뒤 알림(종료 처리는 finished 가 한다)."""
        st, ports = self.st, self.ports
        # ★ 실패 표시·정리를 먼저 한다. 창이 먼저면(확인을 늦게 누르면) 그 사이 종료 처리가 돌아
        #   실패한 스텝이 정상 종료로 기록되고 CSV 다음 스텝이 시작된다 — ALL STOP 과 같은 순서로 맞춘다.
        st.step_ok = False

        # ✅ 저장만 (stop 시퀀스 끝나고 finished에서 카드+일반챗 1줄)
        ports.chat_notify_failed_now(error_message, send_text=False)
        ports.notice("process", "critical", "공정 중단", f"공정이 중단되었습니다.\n\n사유: {error_message}")

        # ✅ 여기서 _handle_process_finished()를 직접 호출하지 마세요.
        # stop 시퀀스가 끝나면 ProcessController.finished가 1번만 호출해줍니다.

    def on_connection_failed(self, error_message: str) -> None:
        """공정 컨트롤러 connection_failed — 컨트롤러가 finished 를 내지 않으므로 여기서 종료 처리까지 한다."""
        st, ports = self.st, self.ports
        # ✅ 실패 원인 저장만 (종료 카드+실패 원인 일반챗은 _handle_process_finished에서)
        ports.chat_notify_failed_now(error_message, send_text=False)

        st.step_ok = False  # 연결 실패도 실패 처리
        self.on_finished()
        ports.notice("process", "critical", "연결 실패", error_message)   # 정리 뒤에 알린다

    def on_restart_required(self, message: str) -> None:
        """장치가 "재시작" 수준으로 알린 경우(PLC 재기동 등) — 실패 표시 후 리스트 취소 또는 공정 정지."""
        st, ports = self.st, self.ports
        st.step_ok = False

        # ✅ 실제 원인(message)을 그대로 남김 (CH1/CH2처럼)
        reason = (message or "").strip() or "PLC 통신 이상(재시작)"
        ports.chat_notify_failed_now(reason, send_text=False)   # 저장만(카드+일반챗은 finished에서)

        # ✅ CSV 리스트가 진행중이면, 다음 스텝이 이어지지 않도록 취소 플래그부터 세팅
        if st.csv_mode and st.csv_rows:
            st.csv_cancelled = True

            # (1) 딜레이 중이거나, 스텝 사이(=process_running False)면: 바로 리스트 취소
            if st.delay_active or (not st.running):
                self.cancel_csv_list_now("CSV 공정 취소", reason=reason)
                return

        # (2) 실제 공정 스텝이 돌고 있으면: stop_process로 안전 종료
        if ports.process_controller_present() and st.running:
            ports.request_stop()
