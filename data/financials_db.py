"""
data/financials_db.py — US/Crypto 재무제표 & 재무비율 로컬 Parquet DB 관리

저장 구조:
  data/local/ohlc_db/us/financials/us_financials_YYYY.parquet
  data/local/ohlc_db/us/ratios/us_ratios_YYYY.parquet
  data/local/ohlc_db/crypto/ratios/crypto_ratios_YYYY.parquet

financials 스키마 (분기별 재무제표):
  Ticker | PeriodDate | Year | Quarter |
  Revenue | GrossProfit | OperatingIncome | NetIncome | EBITDA |
  TotalAssets | TotalLiabilities | Equity |
  OperatingCashFlow | FreeCashFlow | CapEx |
  SnapDate
  PrimaryKey: (Ticker, PeriodDate)

ratios 스키마 (스냅샷, 실행할 때마다 누적):
  Ticker | SnapDate | Name | Sector | Industry |
  MarketCap | SharesOutstanding |
  PE | ForwardPE | PB | PS |
  ROE | ROA | DebtToEquity | Beta |
  DividendYield | EPS | ProfitMargin | OperatingMargin |
  RevenueGrowth | EarningsGrowth | CurrentRatio
  PrimaryKey: (Ticker, SnapDate)

압축: snappy (pyarrow.parquet)
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import config

logger = logging.getLogger(__name__)

_LOCAL_ROOT = Path(config.LOCAL_DATA_DIR) / "ohlc_db"

# ── 스키마 컬럼 정의 ──────────────────────────────────────────────────────────
_FINANCIALS_COLS = [
    "Ticker", "PeriodDate", "Year", "Quarter",
    "Revenue", "GrossProfit", "OperatingIncome", "NetIncome", "EBITDA",
    "TotalAssets", "TotalLiabilities", "Equity",
    "OperatingCashFlow", "FreeCashFlow", "CapEx",
    "SnapDate",
]

_RATIOS_COLS = [
    "Ticker", "SnapDate", "Name", "Sector", "Industry",
    "MarketCap", "SharesOutstanding",
    "PE", "ForwardPE", "PB", "PS",
    "ROE", "ROA", "DebtToEquity", "Beta",
    "DividendYield", "EPS", "ProfitMargin", "OperatingMargin",
    "RevenueGrowth", "EarningsGrowth", "CurrentRatio",
]


# ══════════════════════════════════════════════════════════════════════════════
# 경로 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def local_financials_path(market: str, year: int) -> Path:
    """financials Parquet 파일 경로."""
    return _LOCAL_ROOT / market / "financials" / f"{market}_financials_{year}.parquet"


def local_ratios_path(market: str, year: int) -> Path:
    """ratios Parquet 파일 경로."""
    return _LOCAL_ROOT / market / "ratios" / f"{market}_ratios_{year}.parquet"


# ══════════════════════════════════════════════════════════════════════════════
# 읽기
# ══════════════════════════════════════════════════════════════════════════════

def load_financials_year(market: str, year: int) -> pd.DataFrame:
    """연도별 financials Parquet 로드. 파일 없으면 빈 DataFrame 반환."""
    path = local_financials_path(market, year)
    if not path.exists():
        return pd.DataFrame(columns=_FINANCIALS_COLS)
    try:
        df = pq.read_table(str(path)).to_pandas()
        if "PeriodDate" in df.columns:
            df["PeriodDate"] = pd.to_datetime(df["PeriodDate"]).dt.date
        if "SnapDate" in df.columns:
            df["SnapDate"] = pd.to_datetime(df["SnapDate"]).dt.date
        return df
    except Exception as e:
        logger.error(f"[FinancialsDB] {path.name} 로드 실패: {e}")
        return pd.DataFrame(columns=_FINANCIALS_COLS)


def load_ratios_year(market: str, year: int) -> pd.DataFrame:
    """연도별 ratios Parquet 로드. 파일 없으면 빈 DataFrame 반환."""
    path = local_ratios_path(market, year)
    if not path.exists():
        return pd.DataFrame(columns=_RATIOS_COLS)
    try:
        df = pq.read_table(str(path)).to_pandas()
        if "SnapDate" in df.columns:
            df["SnapDate"] = pd.to_datetime(df["SnapDate"]).dt.date
        return df
    except Exception as e:
        logger.error(f"[FinancialsDB] {path.name} 로드 실패: {e}")
        return pd.DataFrame(columns=_RATIOS_COLS)


# ══════════════════════════════════════════════════════════════════════════════
# 저장 (병합 → 중복제거 → snappy 압축)
# ══════════════════════════════════════════════════════════════════════════════

def save_financials(new_df: pd.DataFrame, market: str):
    """
    financials 저장.
    PeriodDate 기준 연도별 분할 저장.
    기존 파일 병합 → (Ticker, PeriodDate) 기준 중복제거(최신우선) → snappy 저장.
    """
    if new_df.empty:
        logger.warning(f"[FinancialsDB] save_financials: 빈 DataFrame (market={market})")
        return

    df = new_df.copy()
    if "PeriodDate" in df.columns:
        df["PeriodDate"] = pd.to_datetime(df["PeriodDate"]).dt.date
    if "SnapDate" in df.columns:
        df["SnapDate"] = pd.to_datetime(df["SnapDate"]).dt.date

    # 연도별 분할
    df["_year"] = df["PeriodDate"].apply(lambda d: d.year if d is not None else None)
    df = df.dropna(subset=["_year"])
    df["_year"] = df["_year"].astype(int)

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        _save_financials_year(year_df, market, int(year))


def _save_financials_year(df: pd.DataFrame, market: str, year: int):
    """단일 연도 financials 저장 (내부 함수)."""
    path = local_financials_path(market, year)
    path.parent.mkdir(parents=True, exist_ok=True)

    # 기존 파일 병합
    if path.exists():
        try:
            existing = load_financials_year(market, year)
            if not existing.empty:
                df = pd.concat([existing, df], ignore_index=True)
        except Exception as e:
            logger.warning(f"[FinancialsDB] 기존 파일 병합 실패 → 덮어씀: {e}")

    # 중복 제거 (최신 우선)
    if "Ticker" in df.columns and "PeriodDate" in df.columns:
        df = df.drop_duplicates(subset=["Ticker", "PeriodDate"], keep="last")

    # 컬럼 순서 정렬
    cols = [c for c in _FINANCIALS_COLS if c in df.columns]
    extra = [c for c in df.columns if c not in _FINANCIALS_COLS]
    df = df[cols + extra].sort_values(["Ticker", "PeriodDate"]).reset_index(drop=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path), compression="snappy")

    size_kb = path.stat().st_size / 1024
    logger.info(
        f"[FinancialsDB] 저장 완료: {path.name} "
        f"({len(df):,}행, {df['Ticker'].nunique() if 'Ticker' in df.columns else '?'}종목, "
        f"{size_kb:.1f}KB)"
    )


def save_ratios(new_df: pd.DataFrame, market: str):
    """
    ratios 저장.
    SnapDate 기준 연도별 분할 저장.
    기존 파일 병합 → (Ticker, SnapDate) 기준 중복제거(최신우선) → snappy 저장.
    """
    if new_df.empty:
        logger.warning(f"[FinancialsDB] save_ratios: 빈 DataFrame (market={market})")
        return

    df = new_df.copy()
    if "SnapDate" in df.columns:
        df["SnapDate"] = pd.to_datetime(df["SnapDate"]).dt.date

    # 연도별 분할
    df["_year"] = df["SnapDate"].apply(lambda d: d.year if d is not None else None)
    df = df.dropna(subset=["_year"])
    df["_year"] = df["_year"].astype(int)

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        _save_ratios_year(year_df, market, int(year))


def _save_ratios_year(df: pd.DataFrame, market: str, year: int):
    """단일 연도 ratios 저장 (내부 함수)."""
    path = local_ratios_path(market, year)
    path.parent.mkdir(parents=True, exist_ok=True)

    # 기존 파일 병합
    if path.exists():
        try:
            existing = load_ratios_year(market, year)
            if not existing.empty:
                df = pd.concat([existing, df], ignore_index=True)
        except Exception as e:
            logger.warning(f"[FinancialsDB] 기존 ratios 파일 병합 실패 → 덮어씀: {e}")

    # 중복 제거 (최신 우선)
    if "Ticker" in df.columns and "SnapDate" in df.columns:
        df = df.drop_duplicates(subset=["Ticker", "SnapDate"], keep="last")

    # 컬럼 순서 정렬
    cols = [c for c in _RATIOS_COLS if c in df.columns]
    extra = [c for c in df.columns if c not in _RATIOS_COLS]
    df = df[cols + extra].sort_values(["Ticker", "SnapDate"]).reset_index(drop=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path), compression="snappy")

    size_kb = path.stat().st_size / 1024
    logger.info(
        f"[FinancialsDB] ratios 저장 완료: {path.name} "
        f"({len(df):,}행, {df['Ticker'].nunique() if 'Ticker' in df.columns else '?'}종목, "
        f"{size_kb:.1f}KB)"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Drive 연동
# ══════════════════════════════════════════════════════════════════════════════

def _get_uploader(uploader=None):
    """DriveUploader 인스턴스 반환 (인자로 받거나 새로 생성).
    루트 폴더를 GDRIVE_OHLC_FOLDER_ID([Database] Market Crawling Data)로 설정.
    """
    if uploader is not None:
        return uploader
    try:
        from data.drive_uploader import DriveUploader
        return DriveUploader(root_folder_id=config.GDRIVE_OHLC_FOLDER_ID or None)
    except Exception as e:
        logger.error(f"[FinancialsDB] DriveUploader 초기화 실패: {e}")
        return None


def upload_financials(market: str, years: list[int], uploader=None):
    """지정 연도 financials Parquet을 Drive에 업로드."""
    u = _get_uploader(uploader)
    if u is None:
        logger.warning("[FinancialsDB] uploader 없음 → 업로드 건너뜀")
        return

    remote_path = config.DRIVE_PATHS.get("us_financials")
    if not remote_path:
        logger.error("[FinancialsDB] DRIVE_PATHS에 'us_financials' 없음")
        return

    for year in years:
        path = local_financials_path(market, year)
        if not path.exists():
            logger.warning(f"[FinancialsDB] 업로드 대상 없음: {path.name}")
            continue
        try:
            u.upload(str(path), remote_path)
            logger.info(f"[FinancialsDB] 업로드 완료: {path.name}")
        except Exception as e:
            logger.error(f"[FinancialsDB] {path.name} 업로드 실패: {e}")


def upload_ratios(market: str, years: list[int], uploader=None):
    """지정 연도 ratios Parquet을 Drive에 업로드."""
    u = _get_uploader(uploader)
    if u is None:
        logger.warning("[FinancialsDB] uploader 없음 → 업로드 건너뜀")
        return

    # market에 따라 Drive 경로 선택
    if market == "us":
        remote_path = config.DRIVE_PATHS.get("us_ratios")
        drive_key = "us_ratios"
    else:
        remote_path = config.DRIVE_PATHS.get("crypto_ratios")
        drive_key = "crypto_ratios"

    if not remote_path:
        logger.error(f"[FinancialsDB] DRIVE_PATHS에 '{drive_key}' 없음")
        return

    for year in years:
        path = local_ratios_path(market, year)
        if not path.exists():
            logger.warning(f"[FinancialsDB] 업로드 대상 없음: {path.name}")
            continue
        try:
            u.upload(str(path), remote_path)
            logger.info(f"[FinancialsDB] ratios 업로드 완료: {path.name}")
        except Exception as e:
            logger.error(f"[FinancialsDB] {path.name} 업로드 실패: {e}")


def download_financials_all(market: str, uploader=None):
    """Drive에서 financials 모든 연도 파일 다운로드."""
    u = _get_uploader(uploader)
    if u is None:
        return

    remote_path = config.DRIVE_PATHS.get("us_financials")
    if not remote_path:
        logger.error("[FinancialsDB] DRIVE_PATHS에 'us_financials' 없음")
        return

    local_d = _LOCAL_ROOT / market / "financials"
    local_d.mkdir(parents=True, exist_ok=True)

    try:
        u.download_all(remote_path, str(local_d), extensions=(".parquet",))
        logger.info(f"[FinancialsDB] {market} financials 전체 다운로드 완료")
    except Exception as e:
        logger.error(f"[FinancialsDB] {market} financials 다운로드 실패: {e}")


def download_ratios_all(market: str, uploader=None):
    """Drive에서 ratios 모든 연도 파일 다운로드."""
    u = _get_uploader(uploader)
    if u is None:
        return

    if market == "us":
        remote_path = config.DRIVE_PATHS.get("us_ratios")
    else:
        remote_path = config.DRIVE_PATHS.get("crypto_ratios")

    if not remote_path:
        logger.error(f"[FinancialsDB] DRIVE_PATHS에 ratios 경로 없음 (market={market})")
        return

    local_d = _LOCAL_ROOT / market / "ratios"
    local_d.mkdir(parents=True, exist_ok=True)

    try:
        u.download_all(remote_path, str(local_d), extensions=(".parquet",))
        logger.info(f"[FinancialsDB] {market} ratios 전체 다운로드 완료")
    except Exception as e:
        logger.error(f"[FinancialsDB] {market} ratios 다운로드 실패: {e}")
