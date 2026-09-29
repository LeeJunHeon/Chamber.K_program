# lib/paths.py
# -*- coding: utf-8 -*-
"""개발 모드 스위치와 기록 경로를 정하는 단 한 곳.

lib/config_local.py(git 에 없음)에 DEV_MODE = True 가 있으면 개발 모드다.
  - 기록(NAS 로 가던 것 전부)은 저장소 안 _dev_logs/ 로 간다.
  - ERP 리포터·구글챗은 주소·토큰 값과 무관하게 꺼진다(erp_settings / chat_webhook).
DEV_MODE 가 없거나 False 면(장비 노트북) 운영 경로·설정을 그대로 쓴다.

NAS 경로가 필요한 곳은 전부 여기서 가져다 쓴다(lib.logger, lib.heater_logger, lib.config).
"""
from pathlib import Path
from typing import NamedTuple, Tuple

try:
    from lib import config_local as _cfg_local
except Exception:
    _cfg_local = None

# 저장소 루트(= main.py 가 있는 폴더)
REPO_ROOT = Path(__file__).resolve().parent.parent
DEV_LOG_ROOT = REPO_ROOT / "_dev_logs"

# 운영(장비 노트북) 경로
OPS_NAS_LOG_DIR = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\CHK")
OPS_CHK_CSV_PATH = r"\\VanaM_NAS\VanaM_Sputter\Sputter\Calib\Database\ChK_log.csv"

DEV_MODE_BANNER = "개발 모드 — ERP·구글챗 꺼짐, 기록은 _dev_logs"
DEV_MODE_TITLE_TAG = " [개발 모드]"


def read_dev_mode(cfg=None) -> bool:
    """config_local 모듈의 DEV_MODE 가 정확히 True 일 때만 개발 모드."""
    return getattr(cfg, "DEV_MODE", False) is True


class LogPaths(NamedTuple):
    nas_log_dir: Path        # log.txt 가 놓이는 CHK 루트
    process_dir: Path        # 공정 로그
    heater_dir: Path         # 히터 CSV/텍스트 로그
    plc_dir: Path            # PLC 블랙박스
    comm_dir: Path           # COMM_events.csv
    chk_csv_path: str        # 공정 요약 ChK_log.csv


def resolve_paths(dev_mode: bool) -> LogPaths:
    root = DEV_LOG_ROOT if dev_mode else OPS_NAS_LOG_DIR
    chk_csv = str(DEV_LOG_ROOT / "ChK_log.csv") if dev_mode else OPS_CHK_CSV_PATH
    return LogPaths(root, root / "process", root / "heater", root / "plc", root / "comm", chk_csv)


def erp_settings(cfg=None, dev_mode: bool = False) -> Tuple[str, str]:
    """(ERP_INGEST_URL, ERP_INGEST_TOKEN). 개발 모드면 빈 값 → ErpReporter 비활성."""
    if dev_mode or cfg is None:
        return "", ""
    return ((getattr(cfg, "ERP_INGEST_URL", "") or "").strip(),
            (getattr(cfg, "ERP_INGEST_TOKEN", "") or "").strip())


def chat_webhook(cfg=None, dev_mode: bool = False, name: str = "CHAT_WEBHOOK_URL") -> str:
    """구글챗 웹훅 주소. 개발 모드면 빈 값 → 전송 안 함."""
    if dev_mode or cfg is None:
        return ""
    return (getattr(cfg, name, "") or "").strip()


DEV_MODE: bool = read_dev_mode(_cfg_local)
PATHS: LogPaths = resolve_paths(DEV_MODE)

NAS_LOG_DIR = PATHS.nas_log_dir
NAS_PROCESS_LOG_DIR = PATHS.process_dir
NAS_HEATER_LOG_DIR = PATHS.heater_dir
NAS_PLC_LOG_DIR = PATHS.plc_dir
NAS_COMM_LOG_DIR = PATHS.comm_dir
CHK_CSV_PATH = PATHS.chk_csv_path
