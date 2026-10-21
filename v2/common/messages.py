"""장비↔웹 메시지 형식 (장비 공통). v1 reporter 계약과 맞추지 않고 새로 정한다.

전송: 장비가 UPLINK_INTERVAL_MS 마다 HTTPS POST(Bearer 장비별 토큰).
  요청 본문  {"equipment", "instance", "schemaHash", "messages": [Envelope...]}
  응답 본문  {"commands": [Command...], "ackUpTo": <마지막으로 저장한 메시지 seq>}
Envelope:   {"id"(uuid), "seq"(인스턴스 안 증가), "ts"(UTC ISO), "type", "data"}
- 웹이 저장한 seq 까지(ackUpTo)만 장비가 보관함에서 지운다 — 끊겨도 잃지 않는다(state 는 최신 1개만 보관).
- 명령은 응답으로만 받는다(장비가 먼저 연결, 웹은 장비로 접속하지 않는다).
- config_apply 는 명령(kind=CONFIG_APPLY)으로 오고, 결과는 config_ack 메시지로 간다.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MField:
    name: str
    type: str          # str / int / float / bool / obj / list / iso / uuid
    required: bool
    desc: str


@dataclass(frozen=True)
class MessageDef:
    type: str
    direction: str     # "up"(장비→웹) / "down"(웹→장비)
    desc: str
    fields: tuple


def _f(name, type_, required, desc):
    return MField(name, type_, required, desc)


ENVELOPE: tuple = (
    _f("id", "uuid", True, "메시지 id(중복 저장 방지)"),
    _f("seq", "int", True, "인스턴스 안에서 1씩 증가"),
    _f("ts", "iso", True, "UTC 시각"),
    _f("type", "str", True, "메시지 종류"),
    _f("data", "obj", True, "본문"),
)

MESSAGES: tuple = (
    MessageDef("hello", "up", "시작·재연결 때. 웹은 같은 장비의 이전 인스턴스 대기 명령을 만료시킨다", (
        _f("program", "str", True, "프로그램 이름"),
        _f("version", "str", True, "git 태그 또는 SHA"),
        _f("runMode", "str", True, "sim / bench / prod"),
        _f("schemaHash", "str", True, "파라미터 정의 해시(sha256)"),
        _f("schema", "obj", True, "파라미터 정의·키 사이 규칙(비밀값 기본값 제외)"),
        _f("configVersion", "int", True, "장비가 쓰고 있는 설정 버전(0 = 기본값)"),
        _f("snapshotColumns", "list", True, "1초 스냅샷 열 이름"),
        _f("commandKinds", "list", True, "장비가 받는 명령 종류"),
        _f("webSchema", "obj", False, "웹 설정 내보내기 묶음(chk.web_schema.bundle — 설정·레시피·블록·공정 중 수정·정지 순서)"),
        _f("webSchemaHash", "str", True, "묶음 정의 해시(바뀌었을 때만 webSchema 를 싣는다)"),
    )),
    MessageDef("state", "up", "현재 상태 1개(변화가 있으면 매 주기, 없으면 UPLINK_STATE_HEARTBEAT_SEC 마다)", (
        _f("procState", "str", True, "공정 상태 기계 상태"),
        _f("step", "obj", False, "현재 스텝(이름·번호·총수·남은 초·전체 초)"),
        _f("recipe", "obj", False, "적재된 레시피(이름·행)"),
        _f("heaterRecipe", "obj", False, "히터 레시피 진행"),
        _f("snapshot", "obj", True, "가장 최근 1초 행(열 이름 → 값)"),
        _f("alarms", "list", True, "활성 알람(규칙 id·사유 코드·시작 시각)"),
        _f("links", "obj", True, "장비별 통신 상태(품질·나이)"),
        _f("configVersion", "int", True, "적용 중 설정 버전"),
        _f("configPending", "obj", False, "대기 중 설정(idle/restart 대기)"),
    )),
    MessageDef("snapshot_summary", "up", "UPLINK_SUMMARY_PERIOD_SEC(1분) 요약 — 필드별 last/mean/min/max", (
        _f("from", "iso", True, "구간 시작(정각)"),
        _f("to", "iso", True, "구간 끝"),
        _f("rows", "int", True, "구간 행 수(누락 확인용)"),
        _f("values", "obj", True, "열 이름 → 요약값"),
        _f("quality", "obj", True, "장비별 ok 비율·최대 나이"),
    )),
    MessageDef("event", "up", "알람·상태 변화·정보(로그 한 줄마다가 아님 — 의미 있는 사건만)", (
        _f("level", "str", True, "error / warn / info"),
        _f("code", "str", True, "사유 코드 또는 이벤트 코드"),
        _f("ruleId", "str", False, "안전 규칙 id(S..)"),
        _f("message", "str", True, "사람이 읽는 문구(500자)"),
        _f("context", "obj", False, "판정에 쓴 값"),
    )),
    MessageDef("run_start", "up", "공정 시작(스텝 단위)", (
        _f("runId", "uuid", True, "공정 id"),
        _f("recipeName", "str", False, "레시피 이름"),
        _f("stepNo", "int", True, "스텝 번호"),
        _f("params", "obj", True, "이 스텝 파라미터(검사 뒤 값)"),
        _f("configVersion", "int", True, "설정 버전"),
    )),
    MessageDef("run_end", "up", "공정 종료 — run_start 를 보낸 공정에만", (
        _f("runId", "uuid", True, "공정 id"),
        _f("result", "str", True, "done / aborted / error"),
        _f("reasonCode", "str", False, "사유 코드"),
        _f("summary", "obj", True, "공정 요약(평균·최대·베이스 압력·실제 시간 등)"),
    )),
    MessageDef("cmd_result", "up", "명령 결과(accepted/rejected 한 번, done/failed 한 번)", (
        _f("cmdId", "str", True, "명령 id"),
        _f("status", "str", True, "accepted / rejected / done / failed"),
        _f("reasonCode", "str", False, "사유 코드"),
        _f("detail", "str", False, "문구(300자)"),
    )),
    MessageDef("config_ack", "up", "CONFIG_APPLY 결과", (
        _f("version", "int", True, "요청 버전"),
        _f("status", "str", True, "applied / pending_idle / pending_restart / rejected"),
        _f("errors", "list", False, "검사 오류 목록"),
        _f("appliedKeys", "list", False, "바로 적용한 키"),
        _f("pendingKeys", "list", False, "대기 중 키"),
        _f("kind", "str", False, "apply / rollback(CONFIG_ROLLBACK)"),
        _f("by", "str", False, "요청자"),
        _f("changes", "list", False, "[{key, old, new}] — 설정 변경 이력"),
        _f("values", "obj", False, "rollback 이면 새 버전의 값 전체(웹 DB 를 맞춘다)"),
    )),
    MessageDef("command", "down", "웹 → 장비 명령(POST 응답의 commands[])", (
        _f("id", "str", True, "명령 id"),
        _f("kind", "str", True, "명령 종류"),
        _f("args", "obj", True, "인자"),
        _f("requestedBy", "str", True, "요청자"),
        _f("requestedAt", "iso", True, "요청 시각"),
        _f("expiresAt", "iso", True, "유효 시간"),
    )),
)
