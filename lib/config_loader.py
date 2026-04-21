# lib/config_loader.py
"""
config_user.json을 읽어 설정값을 반환합니다.
JSON이 없거나 잘못된 경우 config.py의 기본값을 그대로 사용합니다.
"""
import json
import re
import os

# config_user.json 위치: 프로젝트 루트 (lib/ 한 단계 위)
_JSON_PATH = os.path.join(os.path.dirname(__file__), '..', 'config_user.json')


def _strip_comments(text: str) -> str:
    """JSON 문자열 바깥의 // 주석만 제거합니다."""
    result = []
    for line in text.splitlines():
        in_string = False
        i = 0
        out = []
        while i < len(line):
            c = line[i]
            # 이스케이프되지 않은 " 만 문자열 토글
            if c == '"' and (i == 0 or line[i-1] != '\\'):
                in_string = not in_string
                out.append(c)
            elif not in_string and line[i:i+2] == '//':
                break  # 문자열 밖의 // → 여기서 잘라냄
            else:
                out.append(c)
            i += 1
        result.append(''.join(out))
    return '\n'.join(result)


def load_user_config() -> dict:
    """
    config_user.json을 읽어 dict로 반환합니다.
    - 파일 없음  → 빈 dict 반환 (config.py 기본값 유지)
    - 파싱 실패  → 빈 dict 반환 (config.py 기본값 유지)
    """
    path = os.path.abspath(_JSON_PATH)

    if not os.path.exists(path):
        print("[Config] config_user.json 없음 → config.py 기본값 사용")
        return {}

    try:
        with open(path, encoding='utf-8') as f:
            raw = f.read()

        cleaned = _strip_comments(raw)
        data = json.loads(cleaned)

        # "// ..." 형태의 주석 key 제거
        result = {k: v for k, v in data.items()
                  if not str(k).strip().startswith('//')}

        print(f"[Config] config_user.json 로드 완료 ({len(result)}개 항목)")
        return result

    except json.JSONDecodeError as e:
        print(f"[Config] config_user.json JSON 문법 오류 → config.py 기본값 사용\n  → {e}")
        return {}
    except Exception as e:
        print(f"[Config] config_user.json 읽기 실패 → config.py 기본값 사용\n  → {e}")
        return {}


# 프로그램 시작 시 딱 1회 로드
_USER: dict = load_user_config()


def get(key: str, default):
    """
    JSON에 해당 키가 있으면 JSON 값을 반환하고,
    없으면 default(config.py 기본값)를 반환합니다.
    """
    return _USER.get(key, default)