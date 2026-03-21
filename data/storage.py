"""
data/storage.py — Parquet/ZSTD 저장·읽기 + DuckDB 쿼리

파일 구조 (LOCAL_DATA_DIR 기준):
  market/YYYYMM.parquet     — 월별 시장 스냅샷
  financials/YYYY.parquet   — 연간 재무제표
  prices/YYYYMM.parquet     — 월별 일별 OHLCV
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import config

logger = logging.getLogger(__name__)

_LOCAL = Path(config.LOCAL_DATA_DIR)


# ── 경로 헬퍼 ─────────────────────────────────────────────────────────────────

def _market_path(yyyymm: str) -> Path:
    return _LOCAL / "market" / f"{yyyymm}.parquet"

def _financials_path(year: int | str) -> Path:
    return _LOCAL / "financials" / f"{year}.parquet"

def _prices_path(yyyymm: str) -> Path:
    return _LOCAL / "prices" / f"{yyyymm}.parquet"


# ── 내부 저장 유틸 ────────────────────────────────────────────────────────────

def _save_parquet(df: pd.DataFrame, path: Path, dedup_keys: list[str]):
    """
    DataFrame을 Parquet ZSTD로 저장.
    기존 파일이 있으면 병합 후 dedup_keys 기준 중복 제거 (최신 우선).
    """
    if df.empty:
        logger.warning(f"[Storage] 빈 DataFrame → 저장 건너뜀: {path.name}")
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        try:
            existing = pq.read_table(str(path)).to_pandas()
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=dedup_keys, keep="last")
        except Exception as e:
            logger.warning(f"[Storage] 기존 파일 병합 실패 → 덮어씀: {e}")

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path), compression="zstd")
    logger.info(f"[Storage] 저장 완료: {path.name} ({len(df):,}행)")


# ── 저장 ─────────────────────────────────────────────────────────────────────

def save_market(df: pd.DataFrame, yyyymm: str):
    """월별 시장 스냅샷 저장. dedup: (date, ticker)"""
    _save_parquet(df, _market_path(yyyymm), dedup_keys=["date", "ticker"])

def save_financials(df: pd.DataFrame, year: int):
    """연간 재무제표 저장. dedup: (year, ticker)"""
    _save_parquet(df, _financials_path(year), dedup_keys=["year", "ticker"])

def save_prices(df: pd.DataFrame, yyyymm: str):
    """월별 일별 OHLCV 저장. dedup: (date, ticker)"""
    _save_parquet(df, _prices_path(yyyymm), dedup_keys=["date", "ticker"])


# ── 읽기 ─────────────────────────────────────────────────────────────────────

def load_market(yyyymm: str) -> pd.DataFrame:
    path = _market_path(yyyymm)
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(str(path)).to_pandas()

def load_financials(year: int | str) -> pd.DataFrame:
    path = _financials_path(year)
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(str(path)).to_pandas()

def load_prices(yyyymm: str) -> pd.DataFrame:
    path = _prices_path(yyyymm)
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(str(path)).to_pandas()


# ── DuckDB 쿼리 ───────────────────────────────────────────────────────────────

def query(sql: str) -> pd.DataFrame:
    """
    DuckDB로 Parquet 파일 직접 쿼리.
    예:
      query("SELECT * FROM 'data/local/market/*.parquet' WHERE PBR < 1 AND PBR > 0")
      query("SELECT year, COUNT(*) FROM 'data/local/financials/*.parquet' GROUP BY year")
    """
    con = duckdb.connect()
    try:
        return con.execute(sql).df()
    except Exception as e:
        logger.error(f"[Storage] 쿼리 실패: {e}\nSQL: {sql}")
        return pd.DataFrame()
    finally:
        con.close()


# ── 현황 조회 헬퍼 ────────────────────────────────────────────────────────────

def list_local_files() -> dict:
    """로컬에 저장된 파일 현황 반환"""
    result = {}
    for dtype, subdir in [("market", "market"), ("financials", "financials"), ("prices", "prices")]:
        d = _LOCAL / subdir
        files = sorted(d.glob("*.parquet")) if d.exists() else []
        result[dtype] = [f.stem for f in files]
    return result

def print_local_summary():
    """로컬 저장 현황 출력"""
    files = list_local_files()
    print("\n=== 로컬 저장 현황 ===")
    for dtype, keys in files.items():
        print(f"  {dtype:12s}: {len(keys)}개 — {', '.join(keys[:5])}{'...' if len(keys) > 5 else ''}")
    print("=" * 22)
