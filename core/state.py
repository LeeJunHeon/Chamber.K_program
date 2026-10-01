# core/state.py
# -*- coding: utf-8 -*-
"""공정 상태 값 한 곳 — 화면 없이 도는 순수 데이터.

MainDialog 는 self.proc_state 하나를 들고, 옛 이름(process_running, csv_mode, _csv_delay_active …)은
property 로 이 객체의 필드에 연결된다. 기본값은 옮기기 전 main.py 의 첫 초기화 값과 같다.
이 모듈은 PyQt6·UI·main·controller·device·reporter·lib.logger 를 import 하지 않는다.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Phase(str, Enum):
    IDLE = "idle"                  # 대기
    LOADED = "loaded"              # 레시피 적재, 시작 전
    MANUAL = "manual"              # 수동 공정
    CSV_STEP = "csv_step"          # CSV 공정 스텝
    CSV_DELAY = "csv_delay"        # CSV 대기 스텝
    CSV_BETWEEN = "csv_between"    # CSV 스텝 사이


@dataclass
class ProcessState:
    running: bool = False                  # process_running — 공정 스텝(또는 CSV 딜레이)이 도는 중
    csv_file_path: Optional[str] = None    # 적재한 레시피 파일
    csv_rows: list = field(default_factory=list)
    csv_index: int = -1                    # 실행 중인 행(0부터), -1 = 시작 전
    csv_mode: bool = False                 # CSV 리스트 공정 진행 중
    csv_cancelled: bool = False            # STOP 등으로 리스트 전체 취소
    delay_active: bool = False             # _csv_delay_active — CSV 대기 스텝 진행 중
    delay_total_sec: int = 0
    delay_remaining_sec: int = 0
    delay_name: str = ""
    current_name: str = ""                 # current_process_name — 카드·로그에 쓰는 공정 이름
    last_params: Optional[dict] = None     # _last_params — 이번 공정(스텝) params(ChK_log 행용)
    step_ok: bool = False                  # _chk_process_ok — 이번 공정(스텝)이 정상 종료로 기록될지
    # _proc_origin — 작업을 시작한 쪽("local" | "erp"). 끝날 때까지 유지하고, 중간에 다른 쪽이 STOP 해도 바뀌지 않는다.
    origin: str = "local"
    # _finish_handled — 이번 공정의 종료 처리(카드 + CSV 판정)를 이미 했는가. 시작한 적이 없으면 True.
    finish_handled: bool = True
    # _process_heater_claimed — 이번 공정이 히터를 '소유'하는가 (use_heater 且 heater_temp>0).
    #  히터를 제어하는 주체는 한 번에 하나여야 한다 — 공정이 소유하면 히터 레시피를 못 띄우고,
    #  소유하지 않으면 둘이 함께 돌 수 있다.
    heater_claimed: bool = False
    # 적재된 레시피의 표시 이름(원격이면 받은 이름, 노트북이면 파일 이름). 비어 있으면 파일 이름을 쓴다(B4).
    recipe_name: str = ""

    def is_active(self) -> bool:
        """공정이 도는가 — 스텝 진행 중 / CSV 리스트 진행 중 / CSV 대기 중 어느 하나라도."""
        return bool(self.running) or bool(self.csv_mode) or bool(self.delay_active)

    def phase(self) -> Phase:
        if self.delay_active:
            return Phase.CSV_DELAY
        if self.csv_mode:
            return Phase.CSV_STEP if self.running else Phase.CSV_BETWEEN
        if self.running:
            return Phase.MANUAL
        if self.csv_file_path:
            return Phase.LOADED
        return Phase.IDLE

    def clear_csv_list(self) -> None:
        """CSV 리스트 흔적 정리 — 이 7개만 바꾼다. csv_rows 는 새 리스트(제자리 clear() 금지)."""
        self.csv_mode = False
        self.csv_rows = []
        self.csv_index = -1
        self.csv_file_path = None
        self.current_name = ""
        self.last_params = None
        self.recipe_name = ""
