@echo off
rem 챔버K v2 실행 — 작업 폴더는 이 파일이 있는 저장소(%~dp0). 창을 닫으면 파워 출력만 끄고 끝난다(결정 4). 멈출 때는 Ctrl+C.
rem 파이썬: py -3.13 런처 → 없으면 PATH 의 python. 이 파일에 PC 별 경로를 넣지 않는다(장비 노트북에서 저장소 파일을 고치지 않게).
rem 자동 시작·Chrome 키오스크는 전환 단계에서.
chcp 65001 >nul
title 챔버K v2 — 닫지 마세요
cd /d "%~dp0"
set "PY="
py -3.13 -c "" >nul 2>&1 && set "PY=py -3.13"
if not defined PY (
  python -c "" >nul 2>&1 && set "PY=python"
)
if not defined PY (
  echo [중지] 파이썬을 찾을 수 없음 — py -3.13 런처나 PATH 의 python 이 필요합니다
  pause
  exit /b 1
)
echo 파이썬: %PY%
%PY% -X utf8 -m v2.chk --config-dir "C:\ChamberK_v2\config" %*
echo 종료 코드 %ERRORLEVEL%
pause
