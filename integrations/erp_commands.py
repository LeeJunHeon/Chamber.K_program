# integrations/erp_commands.py
# -*- coding: utf-8 -*-
"""ERP 원격 명령 처리 — 꺼내기(drain) → 하나씩 실행(exec_one) → 결과 보고(cmd_result).

옮기기 전에는 MainDialog.__init__ 안의 중첩 함수(_erp_exec_one, _erp_drain_commands)와
MainDialog._erp_silent_failure / _remote_alert_reason 이었다. 본문의 순서·조건·문구·try 범위는 그대로다.
바깥 일(ERP 리포터, 로그, 모달 창 확인, 버튼 위젯, 공정 시작·정지, 레시피 적재, 히터 명령)은 host 로만 부른다.
히터 명령(HEATER_SV, HEATER_ONOFF, RECIPE_HEATER_RUN, …)은 5단계(히터 서비스)까지 host.exec_heater_command 에 맡긴다.

이 모듈은 PyQt6·UI·main·lib.logger 를 import 하지 않는다(위젯은 host 가 건네주는 객체의 메서드만 부른다).
"""
import csv
import glob
import os
import tempfile
from typing import Any, List, Optional, Protocol, Tuple

from core.params import ManualInputs

# === ERP 원격 명령 실행 (메인 스레드 전용) ===
# 안전: 아래 화이트리스트에 없는 명령은 실행하지 않는다.
#       PLC 버튼은 setChecked로 처리해 로컬 UI 상태와 항상 일치시킨다.
PLC_BUTTONS = {
    "Rotary_button", "RV_button", "FV_button", "MV_button", "Vent_button",
    "Turbo_button", "Ar_Button", "O2_Button", "MS_button",
    "S1_button", "S2_button", "BuzzStop_Button",
    "Door_Button",  # 도어는 상승/하강이 이 버튼 하나로 통합되어 있다
    "ION_button",   # 이오나이저 Remote On
}

# 원격 수동 시작(PROCESS_START) 인자 → 수동 입력칸 원시값(ManualInputs).
#  노트북 입력칸을 먼저 고쳐 쓰지 않는다 — 시작이 통과한 뒤에만 노트북에 보여 준다(B1).
_ARG_CHECKS = {"use_g1": "useG1", "use_g2": "useG2", "use_ar": "useAr", "use_o2": "useO2",
               "use_rf": "useRf", "use_rf_pulse": "useRfPulse", "use_dc": "useDc", "use_dc_delay": "dcDelay"}
_ARG_TEXTS = {"g1_name_text": "g1", "g2_name_text": "g2", "ar_flow_text": "arFlow", "o2_flow_text": "o2Flow",
              "working_pressure_text": "workingPressure", "rf_power_text": "rfPower",
              "rfp_power_text": "rfPulsePower", "rfp_freq_text": "rfPulseFreq", "rfp_duty_text": "rfPulseDuty",
              "dc_power_text": "dcPower", "shutter_delay_text": "shutterDelay", "process_time_text": "processTime"}


def manual_inputs_from_args(args, current_offset: str, current_param: str) -> ManualInputs:
    """원격 PROCESS_START 인자 → ManualInputs.
    - 체크 항목: 키가 없으면 False, 있으면 bool(값)
    - 값 칸: 없거나 None 이면 "", 있으면 str(값)
    - offset/param: 값이 있고 공백이 아니면 그 값(장비 값을 덮어씀), 없거나 빈칸이면 장비 현재 값"""
    a = args or {}

    def _cal(key, current):
        v = a.get(key)
        return str(v) if v is not None and str(v).strip() != "" else current

    kw = {f: (key in a and bool(a.get(key))) for f, key in _ARG_CHECKS.items()}
    kw.update({f: ("" if a.get(key) is None else str(a.get(key))) for f, key in _ARG_TEXTS.items()})
    kw["offset_text"] = _cal("offset", current_offset)
    kw["param_text"] = _cal("param", current_param)
    return ManualInputs(**kw)


# 웹에서 만든 레시피를 CSV 로 저장할 때의 열
PROCESS_RECIPE_COLS = ["Process_name", "Ar", "Ar_flow", "O2", "O2_flow",
                       "working_pressure", "process_time", "shutter_delay",
                       "use_rf_power", "rf_power", "use_dc_power", "dc_power",
                       "use_rf_pulse", "rf_pulse_power", "rf_pulse_freq", "rf_pulse_duty",
                       "use_dc_delay", "use_heater", "heater_temp", "heater_ramp",
                       "gun1", "gun2", "G1 Target", "G2 Target"]
HEATER_RECIPE_COLS = ["step", "target_c", "ramp_c_per_min", "ramp_min",
                      "soak_min", "repeat",
                      "use_ar", "ar_flow", "use_o2", "o2_flow", "wp_mtorr"]


def write_recipe_csv(rows, cols, filename: str) -> str:
    """웹 레시피 행을 <임시 폴더>/vanam_recipe/<filename> 에 utf-8-sig CSV 로 쓰고 경로를 돌려준다."""
    d = os.path.join(tempfile.gettempdir(), "vanam_recipe")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, filename)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})
    return path


def cleanup_web_recipes(keep_path) -> None:
    """<임시 폴더>/vanam_recipe/ 의 예전 process_web_*.csv 를 지운다 — 지금 적재돼 있는 파일(keep_path)만 남긴다.
    지우기 실패(열려 있음 등)는 무시한다(B4)."""
    d = os.path.join(tempfile.gettempdir(), "vanam_recipe")
    keep = os.path.normcase(os.path.abspath(str(keep_path))) if keep_path else None
    for f in glob.glob(os.path.join(d, "process_web_*.csv")):
        try:
            if keep and os.path.normcase(os.path.abspath(f)) == keep:
                continue
            os.remove(f)
        except Exception:
            pass


def alert_reason(alerts) -> str:
    """모인 경고창 문구를 ERP 실패 사유 한 줄로. warning/critical 만 사유가 된다."""
    bits = []
    for kind, title, text in alerts:
        if kind == "information":
            continue
        body = " ".join(str(text).split())
        bits.append(f"{title}: {body}" if title else body)
    return " / ".join(bits)


class ErpCommandHost(Protocol):
    """ErpCommandRunner 가 부르는 바깥 일. main.py 의 _MainErpHost 가 구현한다."""

    def erp_rejected(self) -> bool:
        """ERP 가 다른 챔버K 인스턴스 때문에 이 장비의 보고를 거부하고 있는가."""

    def erp_pop_commands(self) -> list:
        """ERP 에서 받은 명령을 모두 꺼낸다."""

    def erp_cmd_result(self, *args) -> None:
        """명령 결과를 ERP 로 보고한다 — 성공 (id, True), 실패 (id, False, 사유)."""

    def rejected_shown(self) -> bool:
        """ERP 거부(보고 멈춤)를 이미 알렸는가."""

    def set_rejected_shown(self, v: bool) -> None:
        """ERP 거부 알림 여부를 기록한다."""

    def log(self, level: str, msg: str) -> None:
        """로그 한 줄."""

    def modal_open(self) -> bool:
        """장비 화면에 모달 대화상자가 떠 있는가."""

    def begin_remote(self) -> None:
        """원격 명령 문맥을 연다 — 경고창을 띄우지 않고 문구를 모은다(모은 목록은 비운다)."""

    def end_remote(self) -> None:
        """원격 명령 문맥을 닫는다(다시 경고창을 띄운다)."""

    def remote_alerts(self) -> list:
        """원격 명령 처리 중 모인 경고창 (종류, 제목, 본문) 목록."""

    def remote_notes(self) -> list:
        """원격 명령 처리 중 모인 장비 알림 (종류, 제목, 본문) 목록."""

    def widget(self, name: str) -> Any:
        """이름으로 입력칸·버튼 위젯을 얻는다(없으면 None)."""

    def start_process(self) -> None:
        """공정을 시작한다(Start 버튼과 같은 경로) — 적재된 레시피로 시작(RECIPE_PROCESS_START)."""

    def current_rf_cal(self) -> Tuple[str, str]:
        """장비 화면의 RF 보정값 (offset, param) 지금 글자."""

    def start_manual(self, inputs: ManualInputs) -> None:
        """원격 수동 시작 — 입력값으로 시작한다(노트북 입력칸은 시작이 통과한 뒤에만 바뀐다)."""

    def stop_process(self) -> None:
        """공정 STOP(STOP 버튼과 같은 경로)."""

    def all_stop(self) -> None:
        """ALL STOP(비상 정지 버튼과 같은 경로)."""

    def load_recipe_file(self, path: str, name: str = "") -> None:
        """레시피 파일을 적재한다(파일 선택 뒤와 같은 경로). name: 표시 이름(원격 레시피 이름, 없으면 "")."""

    def csv_rows(self) -> list:
        """적재된 레시피 행 목록."""

    def csv_file_path(self) -> Optional[str]:
        """적재된 레시피 파일 경로."""

    def process_active(self) -> bool:
        """공정이 도는가(스텝·CSV 리스트·CSV 대기 중 하나라도)."""

    def heater_pending(self) -> Any:
        """히터 가스·압력 준비 뒤 이어서 할 일(("recipe_start", None) 등) — 준비 중이 아니면 None."""

    def heater_recipe_running(self) -> bool:
        """히터 레시피가 돌고 있는가."""

    def exec_heater_command(self, name: str, args: dict) -> bool:
        """히터 명령을 실행한다. 히터 명령이면 True(실패는 예외), 아니면 False."""


class ErpCommandRunner:
    def __init__(self, host: ErpCommandHost):
        self.host = host

    # ==================== 명령 1건 ====================
    def exec_one(self, c: dict) -> None:
        host = self.host
        name = str(c.get("command", ""))
        args = c.get("args") or {}
        if name in PLC_BUTTONS:
            btn = host.widget(name)
            if btn is None:
                raise RuntimeError(f"버튼 없음: {name}")
            btn.setChecked(bool(args.get("on")))
        elif name == "PROCESS_START":
            # 원격 수동 시작 — 입력칸에 먼저 쓰지 않고 인자로 만든 입력값으로 시작한다.
            #  거부·입력 오류면 노트북 입력칸은 그대로, 시작이 통과한 뒤에만 노트북에 보여 준다.
            inputs = manual_inputs_from_args(args, *host.current_rf_cal())
            host.start_manual(inputs)
        elif name == "PROCESS_STOP":
            host.stop_process()
        elif name == "ALL_STOP":
            host.all_stop()
        elif name == "RECIPE_PROCESS_RUN":
            # 웹에서 만든 공정 레시피를 CSV로 저장하고 기존 CSV 실행 경로를 그대로 사용한다
            rows = args.get("rows") or []
            if not rows:
                raise RuntimeError("레시피 행이 없습니다")
            # 명령마다 다른 파일 이름(B4) — 같은 이름에 덮어쓰면 적재 실패를 경로 비교로 알아챌 수 없다.
            #  예전 파일은 지금 적재돼 있는 것만 남기고 지운다.
            cleanup_web_recipes(host.csv_file_path())
            path = write_recipe_csv(rows, PROCESS_RECIPE_COLS, f"process_web_{c.get('id')}.csv")
            c["_csv_path"] = path        # 적재 결과 확인용(경고창 없이 조용히 return 하는 경로 대비)
            host.load_recipe_file(path, str(args.get("name") or ""))

        elif name == "RECIPE_PROCESS_START":
            # 적재된 CSV 레시피로 공정을 시작한다(장비 앞 Start 버튼과 동일 경로)
            if not host.csv_rows():
                raise RuntimeError("적재된 레시피가 없습니다. 먼저 레시피를 적재하세요.")
            host.start_process()

        elif host.exec_heater_command(name, args):
            pass                         # 히터 명령(5단계까지 MainDialog 가 처리)

        else:
            raise RuntimeError(f"허용되지 않은 명령: {name}")

    # ==================== 꺼내기 → 실행 → 보고 ====================
    def drain(self) -> None:
        host = self.host
        try:
            # ERP 는 장비당 한 인스턴스만 받는다. 다른 챔버K 가 이미 붙어 있으면
            #  리포터가 409 를 받고 보고를 잠시 멈춘 뒤 60초마다 hello 로 재연결을
            #  시도한다 — 사용자에게 멈춤/재개를 각 1회만 알린다.
            if host.erp_rejected() and not host.rejected_shown():
                host.set_rejected_shown(True)
                host.log(
                    "WARN",
                    "[ERP] 다른 챔버K 프로그램이 ERP 에 연결되어 있어 보고를 잠시 멈춥니다. "
                    "60초마다 재연결을 시도합니다.")
            elif not host.erp_rejected() and host.rejected_shown():
                host.set_rejected_shown(False)
                host.log("정보", "[ERP] 보고를 재개했습니다.")

            cmds = host.erp_pop_commands()
            if not cmds:
                return
            # 장비에 모달 대화상자가 떠 있으면 조작이 막히므로 원인을 보고하고 중단한다
            if host.modal_open():
                for c in cmds:
                    host.erp_cmd_result(
                        c.get("id"), False,
                        "장비에 확인 대화상자가 열려 있습니다. 현장에서 닫아주세요.")
                host.log(
                    "WARN", "[원격] 대화상자가 열려 있어 명령을 거부했습니다")
                return
            for c in cmds:
                cid = c.get("id")
                _name = str(c.get("command", ""))
                try:
                    # 원격 실행 동안에는 경고창을 띄우지 않는다 — 문구는 _remote_alerts 에 모여 실패 사유가 된다
                    host.begin_remote()
                    try:
                        self.exec_one(c)
                    finally:
                        host.end_remote()
                    _why = alert_reason(host.remote_alerts())
                    if not _why:
                        _why = self.silent_failure(_name, c)
                        if _why:
                            # 조용한 실패인데 처리 중 알림이 있었으면 그 문구가 더 정확하다
                            _note = " / ".join(f"{t}: " + " ".join(str(x).split())
                                               for k, t, x in host.remote_notes() if k != "information")
                            if _note:
                                _why = _note
                    for _k, _t, _x in list(host.remote_alerts()) + list(host.remote_notes()):
                        if _k == "information":
                            host.log("정보", f"[원격] {_name} 안내 — {_t}: " + " ".join(str(_x).split()))
                    if _why:
                        host.log("ERROR", f"[원격] {_name} 실패: {_why}")
                        host.erp_cmd_result(cid, False, _why)
                    else:
                        host.log("정보", f"[원격] {_name} 실행")
                        host.erp_cmd_result(cid, True)
                except Exception as ex:
                    host.log(
                        "ERROR", f"[원격] {_name} 실패: {ex}")
                    host.erp_cmd_result(cid, False, str(ex))
        except Exception:
            pass

    # ==================== 조용한 실패 확인 ====================
    def silent_failure(self, name: str, c: dict) -> str:
        """경고창 없이 조용히 return 한 경로를 결과로 확인한다. 실패면 사유, 아니면 ""."""
        host = self.host
        try:
            if name in ("PROCESS_START", "RECIPE_PROCESS_START"):
                if not host.process_active():
                    return "공정이 시작되지 않았습니다 (장비 로그 확인)"
            elif name == "RECIPE_HEATER_RUN":
                pend = host.heater_pending()
                waiting = bool(pend and str(pend[0]) == "recipe_start")
                if not (host.heater_recipe_running() or waiting):
                    return "히터 레시피를 시작하지 못했습니다 (장비 로그 확인)"
            elif name == "RECIPE_PROCESS_RUN":
                want = str((c or {}).get("_csv_path") or "")
                cur = str(host.csv_file_path() or "")
                if not host.csv_rows() or (want and cur != want):
                    return "레시피를 적재하지 못했습니다 (장비 로그 확인)"
        except Exception:
            pass
        return ""
