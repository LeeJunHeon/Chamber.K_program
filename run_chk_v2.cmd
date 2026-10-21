@echo off
rem 챔버K v2 실행 — 파이썬·작업 폴더 고정. 창을 닫으면 파워 출력만 끄고 끝난다(결정 4). 멈출 때는 Ctrl+C.
rem 파이썬 경로(PY)는 이 노트북의 설치 위치 — 다른 PC 면 이 줄만 고친다. 자동 시작·Chrome 키오스크는 전환 단계에서.
chcp 65001 >nul
title 챔버K v2 — 닫지 마세요
set "PY=C:\Users\wnsgj\AppData\Local\Programs\Python\Python313\python.exe"
cd /d "%~dp0"
if not exist "%PY%" (
  echo [중지] 파이썬을 찾을 수 없음: %PY%
  pause
  exit /b 1
)
"%PY%" -X utf8 -m v2.chk --config-dir "C:\ChamberK_v2\config"
echo 종료 코드 %ERRORLEVEL%
pause
