"""
config.py — 환경변수 참조 설정
⚠️  모든 민감 정보는 환경변수 참조만 — 절대 하드코딩 금지
"""

import os
import sys

# ── API 키 (GitHub Secrets → 환경변수) ─────────────────────────────────────────
DART_API_KEY      = os.environ.get("DART_API_KEY", "")
GDRIVE_FOLDER_ID  = os.environ.get("GDRIVE_FOLDER_ID", "")
GDRIVE_CREDS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "gdrive_creds.json")

# ── 환경 자동 감지 ─────────────────────────────────────────────────────────────
IN_GITHUB_ACTIONS = os.environ.get("GITHUB_ACTIONS") == "true"
IN_COLAB          = "google.colab" in str(sys.modules)

# ── 로컬 임시 저장 경로 (Actions 실행 중 → Drive 업로드 후 삭제 가능) ─────────
LOCAL_DATA_DIR = os.environ.get("LOCAL_DATA_DIR", "data/local")

# ── Google Drive 폴더 구조 (GDRIVE_FOLDER_ID 하위) ────────────────────────────
DRIVE_PATHS = {
    "market":     "data/market",      # 월별 시장 스냅샷 YYYYMM.parquet
    "financials": "data/financials",  # 연간 재무제표 YYYY.parquet
    "prices":     "data/prices",      # 월별 일별 주가 YYYYMM.parquet
    "progress":   "data/progress",    # 수집 진행 현황 collection_status.json
}

# ── 수집 대상 시장 ─────────────────────────────────────────────────────────────
MARKETS = ["KOSPI", "KOSDAQ"]

# ── 재시도 & 딜레이 설정 ────────────────────────────────────────────────────────
MAX_RETRY    = 3
BACKOFF_BASE = 2            # 지수 백오프: 2^n 초
DELAY_MARKET = (1.5, 3.0)  # 마켓 간 랜덤 딜레이 범위(초)
DELAY_API    = (0.5, 1.0)  # API 호출 간 랜덤 딜레이 범위(초)
DELAY_TICKER = (0.3, 0.7)  # 종목별 딜레이 (CompanyGuide)
DELAY_DAILY  = (1.0, 2.0)  # 일별 주가 수집 딜레이 범위(초)

# ── 수집 옵션 ──────────────────────────────────────────────────────────────────
SKIP_IF_DONE = True  # 이미 수집 완료된 월/연도 스킵
