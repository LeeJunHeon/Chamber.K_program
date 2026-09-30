# core/process_service.py
# -*- coding: utf-8 -*-
"""공정 흐름(시작 → CSV 스텝 진행 → 종료·중단) — Qt·화면·장치를 모르는 서비스.

리팩토링 3단계에서 MainDialog 의 공정 흐름을 세 번에 나눠 이리로 옮긴다.
  3a: 공정 시작·레시피 적재   3b(지금): CSV 스텝·딜레이·리스트 취소   3c: STOP·ALL STOP·종료·오류 처리
Qt 객체(딜레이 QTimer·QElapsedTimer)는 MainDialog 에 남고, 서비스는 ports 로 타이머 시작/정지·경과 시간만 부른다.
바깥 일(경고창, 로그, 단계 표시, 버튼, 챗, ERP, 기록 파일, 장치 신호, 히터 확인)은 전부 ports 로만 부른다.
ports 는 main.py 가 구현한다. 본문의 순서·조건·문구는 옮기기 전 main.py 그대로다.

이 모듈은 PyQt6·UI·main·controller·device·reporter·lib.logger·lib.heater_logger·lib.recipe_io 를 import 하지 않는다.
"""
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


class ProcessService:
    def __init__(self, state: ProcessState, ports: ProcessPorts):
        self.st = state
        self.ports = ports

    # ==================== 공정 시작 ====================
    def start(self) -> None:
        """Start 버튼·원격 PROCESS_START·RECIPE_PROCESS_START 공통. 적재된 레시피가 있으면 CSV 리스트, 없으면 수동 공정."""
        st, ports = self.st, self.ports
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
            params = build_manual_params(ports.read_manual_inputs())

        except (ValueError, TypeError) as e:
            ports.alert("warning", "입력 오류", f"공정 파라미터가 잘못되었습니다:\n{e}")
            return

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
