# core/process_service.py
# -*- coding: utf-8 -*-
"""공정 흐름(시작 → CSV 스텝 진행 → 종료·중단) — Qt·화면·장치를 모르는 서비스.

리팩토링 3단계에서 MainDialog 의 공정 흐름을 세 번에 나눠 이리로 옮긴다.
  3a(지금): 공정 시작·레시피 적재   3b: CSV 스텝·딜레이·리스트 취소   3c: STOP·ALL STOP·종료·오류 처리
바깥 일(경고창, 로그, 단계 표시, 버튼, 챗, ERP, 기록 파일, 장치 신호, 히터 확인)은 전부 ports 로만 부른다.
ports 는 main.py 가 구현한다. 본문의 순서·조건·문구는 옮기기 전 main.py 그대로다.

이 모듈은 PyQt6·UI·main·controller·device·reporter·lib.logger·lib.heater_logger·lib.recipe_io 를 import 하지 않는다.
"""
from pathlib import Path
from typing import Iterable, Optional, Protocol, Sequence

from core.params import ManualInputs, build_manual_params
from core.recipe import csv_rows_use_heater, parse_delay_seconds, row_has_content
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

    def start_next_csv_step(self) -> None:
        """다음 CSV 스텝을 시작한다 — 3b 에서 서비스 안으로 옮길 때까지의 임시 다리."""


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
            ports.start_next_csv_step()
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
