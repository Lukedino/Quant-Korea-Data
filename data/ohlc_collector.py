"""
data/ohlc_collector.py — US 주식/ETF 및 크립토 OHLC 수집 (yfinance)

유니버스:
  - US  : input/us_universe.txt (없으면 내장 기본값 60종목)
  - Crypto: input/crypto_universe.txt (없으면 내장 기본값 30종목)

출력 스키마: Ticker | Date | Open | High | Low | Close | Volume
"""

import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── 기본 유니버스 ─────────────────────────────────────────────────────────────

_DEFAULT_US = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "LLY", "TSM",
    "JPM", "V", "UNH", "XOM", "MA", "JNJ", "PG", "COST", "HD", "NFLX",
    "CRM", "AMD", "INTC", "QCOM", "AMAT", "PLTR", "SMCI", "ARM", "MSTR", "COIN",
    "SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "ARKK", "XLF", "XLE", "XLK",
    "XLV", "XLI", "XLC", "XLRE", "XLU", "XLP", "XLB", "XLY", "GLD", "SLV",
    "TLT", "HYG", "LQD", "EEM", "EFA", "SOXX", "SMH", "KWEB", "MCHI", "FXI",
]

_DEFAULT_CRYPTO = [
    "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD",
    "AVAX-USD", "TRX-USD", "LINK-USD", "DOT-USD", "MATIC-USD", "LTC-USD", "BCH-USD",
    "UNI-USD", "ATOM-USD", "XLM-USD", "FIL-USD", "HBAR-USD", "ICP-USD",
    "NEAR-USD", "APT-USD", "ARB-USD", "OP-USD", "INJ-USD", "SUI-USD", "SEI-USD",
    "FTM-USD", "ALGO-USD", "VET-USD",
]

_UNIVERSE_FILES = {
    "us":     "input/us_universe.txt",
    "crypto": "input/crypto_universe.txt",
}

_BATCH_SIZE = 50  # rate limit 방지


# ══════════════════════════════════════════════════════════════════════════════
# 유니버스 로드
# ══════════════════════════════════════════════════════════════════════════════

def load_tickers(market: str) -> list[str]:
    """
    티커 목록 로드.
    input/{market}_universe.txt 파일이 있으면 사용, 없으면 기본값 반환.
    txt 파일 형식: 한 줄에 티커 1개 (빈 줄/# 주석 무시)
    """
    txt_path = Path(_UNIVERSE_FILES.get(market, ""))
    if txt_path.exists():
        tickers = []
        with open(txt_path, "r", encoding="utf-8") as f:
            for line in f:
                t = line.strip()
                if t and not t.startswith("#"):
                    tickers.append(t)
        if tickers:
            logger.info(f"[OhlcCollector] {market} 유니버스 파일 로드: {len(tickers)}종목 ({txt_path})")
            return tickers
        logger.warning(f"[OhlcCollector] {txt_path} 비어 있음 → 기본값 사용")

    defaults = _DEFAULT_US if market == "us" else _DEFAULT_CRYPTO
    logger.info(f"[OhlcCollector] {market} 기본 유니버스 사용: {len(defaults)}종목")
    return list(defaults)


# ══════════════════════════════════════════════════════════════════════════════
# 핵심 수집 함수
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ohlc_range(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """
    yfinance batch download → flat DataFrame (Ticker | Date | Open | High | Low | Close | Volume)

    - 50개씩 배치 처리 (rate limit 방지)
    - MultiIndex → flat 변환 (단일/복수 ticker 모두 처리)
    - tz 제거, 중복 제거
    - 실패 종목 로깅 후 계속

    Args:
        tickers: 수집할 티커 목록
        start:   시작일 "YYYY-MM-DD"
        end:     종료일 "YYYY-MM-DD" (exclusive, yfinance 관례)
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance를 설치하세요: pip install yfinance")

    try:
        from tqdm import tqdm
        batch_iter = tqdm(
            range(0, len(tickers), _BATCH_SIZE),
            desc=f"OHLC fetch {start[:7]}",
            unit="batch",
        )
    except ImportError:
        batch_iter = range(0, len(tickers), _BATCH_SIZE)

    all_rows: list[pd.DataFrame] = []
    failed: list[str] = []

    for i in batch_iter:
        batch = tickers[i: i + _BATCH_SIZE]
        batch_no = i // _BATCH_SIZE + 1
        total_batches = (len(tickers) + _BATCH_SIZE - 1) // _BATCH_SIZE

        logger.debug(
            f"[OhlcCollector] 배치 {batch_no}/{total_batches} "
            f"({len(batch)}종목, {start}~{end})"
        )

        try:
            raw = yf.download(
                batch,
                start=start,
                end=end,
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
            )
        except Exception as e:
            logger.warning(f"[OhlcCollector] 배치 {batch_no} 다운로드 실패: {e}")
            failed.extend(batch)
            time.sleep(2.0)
            continue

        if raw is None or raw.empty:
            logger.warning(f"[OhlcCollector] 배치 {batch_no} 빈 응답")
            continue

        # ── MultiIndex / 단일 ticker 분기 처리 ──────────────────────────────
        for ticker in batch:
            try:
                df_t = _extract_ticker_df(raw, ticker, len(batch))
                if df_t is None or df_t.empty:
                    continue

                # tz 제거 및 Date 변환
                df_t = df_t.reset_index()
                date_col = df_t.columns[0]  # 'Date' or 'Datetime'
                df_t[date_col] = pd.to_datetime(df_t[date_col])
                if df_t[date_col].dt.tz is not None:
                    df_t[date_col] = df_t[date_col].dt.tz_localize(None)
                df_t[date_col] = df_t[date_col].dt.date
                df_t = df_t.rename(columns={date_col: "Date"})

                # 컬럼 정규화
                df_t = df_t.rename(columns={
                    "open": "Open", "high": "High", "low": "Low",
                    "close": "Close", "volume": "Volume",
                })

                df_t["Ticker"] = ticker
                keep = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
                df_t = df_t[[c for c in keep if c in df_t.columns]]

                # Close 유효한 행만
                df_t = df_t.dropna(subset=["Close"])
                df_t = df_t[df_t["Close"] > 0]

                if not df_t.empty:
                    all_rows.append(df_t)

            except Exception as e:
                logger.debug(f"[OhlcCollector] {ticker} 처리 실패: {e}")
                failed.append(ticker)

        # 배치 간 딜레이 (rate limit)
        time.sleep(1.0)

    if failed:
        logger.warning(f"[OhlcCollector] 실패 종목 ({len(failed)}개): {failed[:20]}")

    if not all_rows:
        logger.warning(f"[OhlcCollector] {start}~{end} 수집 결과 없음")
        return pd.DataFrame(columns=["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"])

    result = pd.concat(all_rows, ignore_index=True)

    # 중복 제거
    result = result.drop_duplicates(subset=["Ticker", "Date"], keep="last")
    result = result.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    logger.info(
        f"[OhlcCollector] 수집 완료: {start}~{end} "
        f"→ {len(result):,}행, {result['Ticker'].nunique()}종목"
    )
    return result


def _extract_ticker_df(raw: pd.DataFrame, ticker: str, batch_size: int) -> Optional[pd.DataFrame]:
    """
    yfinance 응답에서 단일 ticker DataFrame 추출.
    단일 ticker / MultiIndex 양쪽 처리.
    """
    if batch_size == 1:
        # 단일 ticker: 일반 DataFrame
        return raw.copy()

    # 복수 ticker: MultiIndex columns
    cols = raw.columns
    if isinstance(cols, pd.MultiIndex):
        # columns: (metric, ticker) 형태
        # level 확인
        level_0_vals = cols.get_level_values(0).unique().tolist()
        level_1_vals = cols.get_level_values(1).unique().tolist()

        if ticker in level_1_vals:
            # (metric, ticker) → xs로 추출
            try:
                df_t = raw.xs(ticker, axis=1, level=1)
                return df_t
            except Exception:
                pass

        if ticker in level_0_vals:
            # (ticker, metric) → df[ticker]
            try:
                return raw[ticker].copy()
            except Exception:
                pass

        return None

    # 단순 컬럼 (ticker가 최상위 레벨)
    if ticker in cols:
        return raw[ticker].copy()

    return None


# ══════════════════════════════════════════════════════════════════════════════
# 백필 (초기 적재)
# ══════════════════════════════════════════════════════════════════════════════

def backfill_market(
    market: str,
    start_year: int,
    end_year: int,
    tickers: Optional[list[str]] = None,
    upload: bool = True,
):
    """
    연도별 루프로 전체 기간 OHLC 백필.

    Args:
        market:     "us" 또는 "crypto"
        start_year: 수집 시작 연도 (포함)
        end_year:   수집 종료 연도 (포함)
        tickers:    None이면 load_tickers()로 자동 로드
        upload:     True면 연도별 저장 후 Drive 업로드
    """
    from data import ohlc_db

    if tickers is None:
        tickers = load_tickers(market)

    logger.info(
        f"[OhlcCollector] {market.upper()} 백필 시작: "
        f"{start_year}~{end_year}년 / {len(tickers)}종목"
    )

    all_dates: list[date] = []

    for year in range(start_year, end_year + 1):
        start_str = f"{year}-01-01"
        end_str   = f"{year + 1}-01-01"  # yfinance end is exclusive

        logger.info(f"[OhlcCollector] {market.upper()} {year}년 수집 중...")
        try:
            df = fetch_ohlc_range(tickers, start_str, end_str)
        except Exception as e:
            logger.error(f"[OhlcCollector] {market} {year}년 수집 실패: {e}")
            continue

        if df.empty:
            logger.warning(f"[OhlcCollector] {market} {year}년 데이터 없음")
            continue

        ohlc_db.save_year(df, market, year)

        if "Date" in df.columns:
            all_dates.extend(df["Date"].tolist())

        if upload:
            ohlc_db.upload_years(market, [year])

    # 상태 업데이트
    if all_dates:
        last_dt  = max(all_dates)
        oldest_dt = min(all_dates)
        ticker_count = len(tickers)
        ohlc_db.update_status(market, last_dt, ticker_count, oldest_dt)
        if upload:
            ohlc_db.upload_status()

    logger.info(f"[OhlcCollector] {market.upper()} 백필 완료: {start_year}~{end_year}년")


# ══════════════════════════════════════════════════════════════════════════════
# 증분 업데이트
# ══════════════════════════════════════════════════════════════════════════════

def update_market(
    market: str,
    tickers: Optional[list[str]] = None,
    upload: bool = True,
):
    """
    마지막 업데이트 이후 데이터를 증분 수집.

    1. Drive에서 db_status.json 다운로드
    2. last_date 파싱 (없으면 오늘 - 1년)
    3. start = last_date + 1일, end = 오늘
    4. start >= end 이면 "이미 최신" 로그 후 반환
    5. 현재 연도 parquet을 Drive에서 다운로드 (로컬에 없으면)
    6. fetch_ohlc_range 수집
    7. append_rows + Drive 업로드
    8. update_status 갱신
    """
    from data import ohlc_db

    if tickers is None:
        tickers = load_tickers(market)

    # 1. Drive에서 status 다운로드
    ohlc_db.download_status()

    # 2. last_date 파싱
    status = ohlc_db.load_status()
    market_status = status.get(market, {})
    last_date_str = market_status.get("last_updated")

    if last_date_str:
        try:
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(f"[OhlcCollector] last_updated 파싱 실패: {last_date_str} → 1년 전 사용")
            last_date = date.today() - timedelta(days=365)
    else:
        last_date = date.today() - timedelta(days=365)
        logger.info(f"[OhlcCollector] {market} status 없음 → 기준일: {last_date}")

    # 3. 수집 범위
    start_date = last_date + timedelta(days=1)
    end_date   = date.today()

    # 4. 이미 최신이면 종료
    if start_date >= end_date:
        logger.info(
            f"[OhlcCollector] {market.upper()} 이미 최신 상태 "
            f"(last={last_date}, today={end_date})"
        )
        return

    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")

    logger.info(
        f"[OhlcCollector] {market.upper()} 증분 업데이트: "
        f"{start_str} ~ {end_str} / {len(tickers)}종목"
    )

    # 5. 현재 연도 parquet이 로컬에 없으면 Drive에서 다운로드
    current_year = end_date.year
    if not ohlc_db.local_path(market, current_year).exists():
        logger.info(f"[OhlcCollector] {market}_{current_year}.parquet 로컬 없음 → Drive 다운로드 시도")
        ohlc_db.download_year(market, current_year)

    # 6. 수집
    try:
        new_df = fetch_ohlc_range(tickers, start_str, end_str)
    except Exception as e:
        logger.error(f"[OhlcCollector] {market} 증분 수집 실패: {e}")
        return

    if new_df.empty:
        logger.warning(f"[OhlcCollector] {market} 증분 수집 결과 없음")
        return

    # 7. append + 업로드
    updated_years = ohlc_db.append_rows(new_df, market)
    if upload and updated_years:
        ohlc_db.upload_years(market, updated_years)

    # 8. 상태 갱신
    if "Date" in new_df.columns:
        actual_last = new_df["Date"].max()
        actual_oldest_str = market_status.get("oldest_date")
        actual_oldest: Optional[date] = None
        if actual_oldest_str:
            try:
                actual_oldest = datetime.strptime(actual_oldest_str, "%Y-%m-%d").date()
            except ValueError:
                pass
        ohlc_db.update_status(market, actual_last, len(tickers), actual_oldest)
        if upload:
            ohlc_db.upload_status()

    logger.info(f"[OhlcCollector] {market.upper()} 증분 업데이트 완료")
