"""
data/kr_db.py — KR 시장 OHLC+시총 연도별 Parquet DB 관리

저장 구조:
  data/local/ohlc_db/kr/marcap-YYYY.parquet

스키마 (marcap 표준):
  Code | Name | Close | Dept | ChangeCode | Changes | ChangesRatio |
  Volume | Amount | Open | High | Low | Marcap | Stocks |
  Market | MarketId | Rank | Date

  - Code     : 종목코드 6자리 문자열
  - Market   : KOSPI / KOSDAQ / KONEX / KOSDAQ GLOBAL
  - MarketId : STK / KSQ / KNX
  - Marcap   : 시가총액 (원)
  - Rank     : 시총 순위 (시장 내)
  - Date     : datetime64[ns]

Primary Key: (Code, Date)
압축: snappy
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import config

logger = logging.getLogger(__name__)

_LOCAL_ROOT = Path(config.LOCAL_DATA_DIR) / "ohlc_db" / "kr"
_STATUS_PATH = Path(config.LOCAL_DATA_DIR) / "ohlc_db" / "_meta" / "kr_status.json"

SCHEMA_COLS = [
    "Code", "Name", "Close", "Dept", "ChangeCode", "Changes", "ChangesRatio",
    "Volume", "Amount", "Open", "High", "Low", "Marcap", "Stocks",
    "Market", "MarketId", "Rank", "Date",
]


# ══════════════════════════════════════════════════════════════════════════════
# 경로 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def local_path(year: int) -> Path:
    return _LOCAL_ROOT / f"marcap-{year}.parquet"


# ══════════════════════════════════════════════════════════════════════════════
# 읽기 / 저장
# ══════════════════════════════════════════════════════════════════════════════

def load_year(year: int) -> pd.DataFrame:
    """연도별 Parquet 로드. 파일 없으면 빈 DataFrame 반환."""
    path = local_path(year)
    if not path.exists():
        return pd.DataFrame(columns=SCHEMA_COLS)
    try:
        df = pq.read_table(str(path)).to_pandas()
        df["Date"] = pd.to_datetime(df["Date"])
        return df
    except Exception as e:
        logger.error(f"[KrDB] {path.name} 로드 실패: {e}")
        return pd.DataFrame(columns=SCHEMA_COLS)


def save_year(df: pd.DataFrame, year: int):
    """
    연도별 Parquet 저장.
    기존 파일이 있으면 병합 후 (Code, Date) 기준 중복 제거 (최신 우선).
    """
    if df.empty:
        logger.warning(f"[KrDB] 빈 DataFrame → 저장 건너뜀: marcap-{year}")
        return

    path = local_path(year)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])

    if path.exists():
        try:
            existing = load_year(year)
            if not existing.empty:
                df = pd.concat([existing, df], ignore_index=True)
        except Exception as e:
            logger.warning(f"[KrDB] 기존 파일 병합 실패 → 덮어씀: {e}")

    df = df.drop_duplicates(subset=["Code", "Date"], keep="last")

    cols = [c for c in SCHEMA_COLS if c in df.columns]
    df = df[cols].sort_values(["Date", "Code"]).reset_index(drop=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path), compression="snappy")

    size_kb = path.stat().st_size / 1024
    logger.info(
        f"[KrDB] 저장 완료: {path.name} "
        f"({len(df):,}행, {df['Date'].dt.date.nunique()}거래일, {size_kb:.1f}KB)"
    )


def append_rows(new_df: pd.DataFrame) -> list[int]:
    """
    새 데이터를 연도별로 분할하여 기존 파일에 append.
    Returns: 업데이트된 연도 목록
    """
    if new_df.empty:
        return []

    df = new_df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df["_year"] = df["Date"].dt.year
    updated_years = []

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        save_year(year_df, int(year))
        updated_years.append(int(year))

    return sorted(updated_years)


def get_last_date(year: Optional[int] = None) -> Optional[date]:
    """저장된 데이터 중 가장 최근 Date 반환."""
    if not _LOCAL_ROOT.exists():
        return None

    if year:
        files = [local_path(year)] if local_path(year).exists() else []
    else:
        files = sorted(_LOCAL_ROOT.glob("marcap-*.parquet"), reverse=True)

    for pfile in files:
        try:
            df = pq.read_table(str(pfile), columns=["Date"]).to_pandas()
            if df.empty:
                continue
            return pd.to_datetime(df["Date"]).max().date()
        except Exception as e:
            logger.warning(f"[KrDB] {pfile.name} Date 조회 실패: {e}")

    return None


# ══════════════════════════════════════════════════════════════════════════════
# 상태 관리
# ══════════════════════════════════════════════════════════════════════════════

def load_status() -> dict:
    if not _STATUS_PATH.exists():
        return {}
    try:
        with open(_STATUS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"[KrDB] status 로드 실패: {e}")
        return {}


def save_status(last_date: date, trading_days: int):
    _STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    status = {
        "last_updated": str(last_date),
        "trading_days_total": trading_days,
        "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
    }
    try:
        with open(_STATUS_PATH, "w", encoding="utf-8") as f:
            json.dump(status, f, indent=2, ensure_ascii=False)
        logger.info(f"[KrDB] status 저장: last={last_date}, days={trading_days}")
    except Exception as e:
        logger.error(f"[KrDB] status 저장 실패: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Drive 연동
# ══════════════════════════════════════════════════════════════════════════════

def _get_uploader(uploader=None):
    if uploader is not None:
        return uploader
    try:
        from data.drive_uploader import DriveUploader
        return DriveUploader(root_folder_id=config.GDRIVE_OHLC_FOLDER_ID or None)
    except Exception as e:
        logger.error(f"[KrDB] DriveUploader 초기화 실패: {e}")
        return None


def upload_years(years: list[int], uploader=None):
    """지정 연도 Parquet을 Drive kr/ 폴더에 업로드."""
    u = _get_uploader(uploader)
    if u is None:
        return

    remote_path = config.DRIVE_PATHS.get("ohlc_kr")
    for year in years:
        path = local_path(year)
        if not path.exists():
            logger.warning(f"[KrDB] 업로드 대상 없음: {path.name}")
            continue
        try:
            u.upload(str(path), remote_path)
            logger.info(f"[KrDB] Drive 업로드 완료: {path.name}")
        except Exception as e:
            logger.error(f"[KrDB] {path.name} 업로드 실패: {e}")


def download_year(year: int, uploader=None) -> bool:
    """Drive에서 연도별 Parquet 다운로드."""
    u = _get_uploader(uploader)
    if u is None:
        return False

    remote_path = config.DRIVE_PATHS.get("ohlc_kr")
    filename = f"marcap-{year}.parquet"
    dest = local_path(year)
    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        u.download(remote_path, filename, str(dest))
        logger.info(f"[KrDB] Drive 다운로드 완료: {filename}")
        return True
    except FileNotFoundError:
        logger.debug(f"[KrDB] Drive에 없음: {remote_path}/{filename}")
        return False
    except Exception as e:
        logger.error(f"[KrDB] {filename} 다운로드 실패: {e}")
        return False


def download_all(uploader=None):
    """Drive kr/ 폴더의 모든 parquet 다운로드."""
    u = _get_uploader(uploader)
    if u is None:
        return

    remote_path = config.DRIVE_PATHS.get("ohlc_kr")
    _LOCAL_ROOT.mkdir(parents=True, exist_ok=True)

    try:
        u.download_all(remote_path, str(_LOCAL_ROOT), extensions=(".parquet",))
        logger.info("[KrDB] Drive 전체 다운로드 완료")
    except Exception as e:
        logger.error(f"[KrDB] 전체 다운로드 실패: {e}")
