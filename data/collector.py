"""
data/collector.py — 원자 수집 함수 모음

데이터 소스:
  - pykrx       : 시장 스냅샷 (PER/PBR/시가총액/종가), 일별 OHLCV
  - DART OpenAPI: 연간 재무제표 (OpenDartReader)
  - CompanyGuide: 재무비율 크롤링 (ROE/ROA/EV_EBITDA 등)

⚠️  pykrx get_market_ohlcv_by_ticker 는 GitHub Actions(Azure IP)에서만 작동.
    Colab(GCP IP)에서는 KRX가 차단 → prices 수집은 반드시 Actions에서 실행.
"""

import logging
import random
import re
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

import config

logger = logging.getLogger(__name__)

# ── pykrx 임포트 (선택적) ─────────────────────────────────────────────────────
try:
    from pykrx import stock as krx
    logger.debug("[Collector] pykrx 로드 완료")
except ImportError:
    krx = None
    logger.warning("[Collector] pykrx 미설치 — 시장 데이터 수집 불가")

# ── OpenDartReader 임포트 (선택적) ────────────────────────────────────────────
try:
    import OpenDartReader
    logger.debug("[Collector] OpenDartReader 로드 완료")
except ImportError:
    OpenDartReader = None
    logger.warning("[Collector] OpenDartReader 미설치 — DART 수집 불가")

# ── yfinance 임포트 (선택적) ──────────────────────────────────────────────────
try:
    import yfinance as yf
    logger.debug("[Collector] yfinance 로드 완료")
except ImportError:
    yf = None
    logger.warning("[Collector] yfinance 미설치 — 일별 주가 수집 불가")


# ══════════════════════════════════════════════════════════════════════════════
# [0] 유틸리티
# ══════════════════════════════════════════════════════════════════════════════

def _safe_float(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", str(text))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _retry(fn, *args, label: str = "", **kwargs):
    """재시도 3회 + 지수 백오프. 성공 시 결과 반환, 모두 실패 시 None 반환."""
    for attempt in range(config.MAX_RETRY):
        try:
            if attempt > 0:
                wait = config.BACKOFF_BASE ** attempt + random.uniform(1, 3)
                logger.info(f"[Retry] {label} 재시도 {attempt}/{config.MAX_RETRY - 1} ({wait:.1f}초 대기)")
                time.sleep(wait)
            return fn(*args, **kwargs)
        except Exception as e:
            logger.warning(f"[Retry] {label} 시도 {attempt + 1} 실패: {e}")
    logger.error(f"[Retry] {label} 최대 재시도 초과")
    return None


def get_last_business_day(date: Optional[str] = None) -> str:
    """주어진 날짜(YYYYMMDD) 또는 오늘 기준 최근 영업일 반환."""
    if krx is None:
        target = datetime.today() if date is None else datetime.strptime(date, "%Y%m%d")
        return target.strftime("%Y%m%d")

    target = datetime.today() if date is None else datetime.strptime(date, "%Y%m%d")
    for i in range(10):
        d = (target - timedelta(days=i)).strftime("%Y%m%d")
        try:
            tickers = krx.get_market_ticker_list(d, market="KOSPI")
            if tickers:
                return d
        except Exception:
            continue
    return target.strftime("%Y%m%d")


# ══════════════════════════════════════════════════════════════════════════════
# [1] pykrx — 유니버스 & 시장 스냅샷
# ══════════════════════════════════════════════════════════════════════════════

def get_universe(date: str) -> list[str]:
    """KOSPI + KOSDAQ 전종목 ticker 리스트 반환."""
    if krx is None:
        raise ImportError("pykrx를 설치하세요: pip install pykrx")
    tickers = []
    for market in config.MARKETS:
        try:
            t = krx.get_market_ticker_list(date, market=market)
            tickers.extend(t)
            time.sleep(random.uniform(*config.DELAY_API))
        except Exception as e:
            logger.error(f"[Collector] {market} 종목 목록 조회 실패: {e}")
    return list(set(tickers))


def get_market_snapshot(date: str) -> pd.DataFrame:
    """
    특정 날짜의 KOSPI+KOSDAQ 전종목 스냅샷 수집.

    Returns DataFrame:
      date, ticker, name, market, close, volume, market_cap, PER, PBR, EPS, BPS, DIV
    """
    if krx is None:
        raise ImportError("pykrx를 설치하세요: pip install pykrx")

    all_data = []

    for market in config.MARKETS:
        logger.info(f"[Collector] {market} {date} 스냅샷 수집 중...")
        df = _collect_market_single(date, market)
        if df is not None and not df.empty:
            df["market"] = market
            all_data.append(df)
        time.sleep(random.uniform(*config.DELAY_MARKET))

    if not all_data:
        logger.warning(f"[Collector] {date} 시장 데이터 없음")
        return pd.DataFrame()

    result = pd.concat(all_data, ignore_index=True)
    logger.info(f"[Collector] {date} 스냅샷 수집 완료: {len(result)}종목")
    return result


def _collect_market_single(date: str, market: str) -> Optional[pd.DataFrame]:
    """단일 마켓(KOSPI/KOSDAQ) 스냅샷 수집. 재시도 포함.

    ⚠️  get_market_cap 엔드포인트는 Azure IP에서 KRX가 차단 → get_market_ohlcv_by_ticker 기본 사용.
        시가총액(market_cap)은 get_market_cap 선택적 시도, 실패 시 None으로 저장.
    """
    for attempt in range(config.MAX_RETRY):
        try:
            if attempt > 0:
                wait = config.BACKOFF_BASE ** attempt + random.uniform(1, 3)
                logger.info(f"[Collector] {market} {date} 재시도 {attempt} ({wait:.1f}초)")
                time.sleep(wait)

            # OHLCV — Azure IP에서 동작 확인된 엔드포인트
            df_ohlcv = krx.get_market_ohlcv_by_ticker(date, market=market)

            if df_ohlcv is None or df_ohlcv.empty:
                logger.warning(f"[Collector] {market} {date} OHLCV 빈 응답 → 재시도")
                continue

            required = ["종가", "거래량"]
            missing = [c for c in required if c not in df_ohlcv.columns]
            if missing:
                logger.warning(f"[Collector] {market} {date} 누락 컬럼: {missing} → 재시도")
                continue

            time.sleep(random.uniform(*config.DELAY_API))

            # PER/PBR/EPS/BPS/DIV (선택적)
            df_fund = pd.DataFrame()
            try:
                df_fund = krx.get_market_fundamental(date, market=market)
                if df_fund is None or df_fund.empty:
                    df_fund = pd.DataFrame()
            except Exception as e:
                logger.warning(f"[Collector] {market} {date} 펀더멘털 수집 실패: {e}")

            time.sleep(random.uniform(*config.DELAY_API))

            # 시가총액 (선택적 — Azure에서 차단될 수 있음)
            df_cap = pd.DataFrame()
            try:
                df_cap_raw = krx.get_market_cap(date, market=market)
                if df_cap_raw is not None and not df_cap_raw.empty and "시가총액" in df_cap_raw.columns:
                    df_cap = df_cap_raw[["시가총액"]].copy()
            except Exception:
                pass  # 시가총액 없이 진행

            time.sleep(random.uniform(*config.DELAY_API))

            # 종목명
            tickers = krx.get_market_ticker_list(date, market=market)
            names = {}
            for t in tickers:
                try:
                    names[t] = krx.get_market_ticker_name(t)
                except Exception:
                    names[t] = ""

            # 합치기
            df = df_ohlcv.copy()
            if not df_fund.empty:
                df = df.join(df_fund, how="left")
            if not df_cap.empty:
                df = df.join(df_cap, how="left")

            df.index.name = "ticker"
            df = df.reset_index()
            df["date"] = date
            df["name"] = df["ticker"].map(names).fillna("")

            # 컬럼 정리
            rename_map = {
                "시가": "open", "고가": "high", "저가": "low",
                "종가": "close", "거래량": "volume", "거래대금": "trading_value",
                "시가총액": "market_cap",
                "PER": "PER", "PBR": "PBR", "EPS": "EPS", "BPS": "BPS", "DIV": "DIV",
            }
            df = df.rename(columns=rename_map)
            keep = ["date", "ticker", "name", "open", "high", "low",
                    "close", "volume", "trading_value", "market_cap",
                    "PER", "PBR", "EPS", "BPS", "DIV"]
            df = df[[c for c in keep if c in df.columns]]
            df = df[df["close"] > 0]  # 거래정지 제외

            return df

        except Exception as e:
            logger.error(f"[Collector] {market} {date} 시도 {attempt + 1} 실패: {e}")

    return None


# ══════════════════════════════════════════════════════════════════════════════
# [2] pykrx — 일별 OHLCV (GitHub Actions 전용)
# ══════════════════════════════════════════════════════════════════════════════

def get_daily_prices_month(yyyymm: str) -> pd.DataFrame:
    """
    특정 월의 전체 일별 OHLCV 수집 (전종목).

    yfinance (Yahoo Finance) 사용 — KRX 전종목시세 엔드포인트 차단 우회.
    종목 목록은 pykrx get_market_ticker_list로 조회.

    Returns DataFrame: date, ticker, open, high, low, close, volume
    """
    if yf is None:
        raise ImportError("yfinance를 설치하세요: pip install yfinance")
    if krx is None:
        raise ImportError("pykrx를 설치하세요: pip install pykrx")

    year  = int(yyyymm[:4])
    month = int(yyyymm[4:6])
    start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        end_dt = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = datetime(year, month + 1, 1) - timedelta(days=1)
    end   = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")  # yfinance end 는 exclusive

    logger.info(f"[Collector] 일별 주가 수집 (yfinance): {yyyymm} ({start}~{end_dt.strftime('%Y-%m-%d')})")

    # 종목 목록 (KRX ticker → Yahoo suffix)
    ticker_map: dict[str, str] = {}  # "005930.KS" → "005930"
    suffix_map = {"KOSPI": ".KS", "KOSDAQ": ".KQ"}
    ref_date = f"{year:04d}{month:02d}01"
    for market in config.MARKETS:
        try:
            tlist = krx.get_market_ticker_list(ref_date, market=market)
            sfx = suffix_map.get(market, ".KS")
            for t in tlist:
                ticker_map[f"{t}{sfx}"] = t
            time.sleep(random.uniform(*config.DELAY_API))
        except Exception as e:
            logger.error(f"[Collector] {market} 종목 목록 조회 실패: {e}")

    if not ticker_map:
        logger.warning(f"[Collector] {yyyymm} 종목 목록 없음")
        return pd.DataFrame()

    yf_tickers = list(ticker_map.keys())
    logger.info(f"[Collector] {yyyymm} 종목 수: {len(yf_tickers)}")

    # yfinance 배치 다운로드 (500종목씩)
    BATCH = 500
    all_rows: list[pd.DataFrame] = []

    for i in range(0, len(yf_tickers), BATCH):
        batch = yf_tickers[i : i + BATCH]
        batch_no = i // BATCH + 1
        total_batches = (len(yf_tickers) + BATCH - 1) // BATCH
        logger.info(f"[Collector] {yyyymm} 배치 {batch_no}/{total_batches} ({len(batch)}종목)")
        try:
            raw = yf.download(
                batch, start=start, end=end,
                auto_adjust=True, progress=False,
                group_by="ticker", threads=True,
            )
            if raw is None or raw.empty:
                logger.warning(f"[Collector] {yyyymm} 배치 {batch_no} 빈 응답")
                continue

            for yf_t in batch:
                krx_t = ticker_map[yf_t]
                try:
                    df_t = raw[yf_t].copy() if len(batch) > 1 else raw.copy()
                    df_t = df_t.dropna(subset=["Close"])
                    if df_t.empty:
                        continue
                    df_t = df_t.reset_index()
                    df_t = df_t.rename(columns={
                        "Date": "date", "Open": "open", "High": "high",
                        "Low": "low", "Close": "close", "Volume": "volume",
                    })
                    df_t["date"] = pd.to_datetime(df_t["date"]).dt.strftime("%Y%m%d")
                    df_t["ticker"] = krx_t
                    keep = ["date", "ticker", "open", "high", "low", "close", "volume"]
                    df_t = df_t[[c for c in keep if c in df_t.columns]]
                    df_t = df_t[df_t["close"] > 0]
                    if not df_t.empty:
                        all_rows.append(df_t)
                except Exception as e:
                    logger.debug(f"[Collector] {yf_t} 처리 실패: {e}")

        except Exception as e:
            logger.warning(f"[Collector] {yyyymm} 배치 {batch_no} 다운로드 실패: {e}")

        time.sleep(1.0)  # Yahoo Finance 레이트 리밋

    if not all_rows:
        logger.warning(f"[Collector] {yyyymm} 일별 주가 수집 결과 없음")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    logger.info(f"[Collector] {yyyymm} 일별 주가 수집 완료: {len(result):,}건")
    return result


def _get_trading_days(start: str, end: str) -> list[str]:
    """KODEX200 기준 거래일 목록 조회."""
    try:
        ref = krx.get_market_ohlcv(start, end, "069500")
        if ref is None or ref.empty:
            return []
        return [d.strftime("%Y%m%d") for d in ref.index]
    except Exception as e:
        logger.error(f"[Collector] 거래일 조회 실패: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# [3] DART OpenAPI — 연간 재무제표
# ══════════════════════════════════════════════════════════════════════════════

def _get_dart():
    """OpenDartReader 인스턴스 반환."""
    if OpenDartReader is None:
        raise ImportError("OpenDartReader를 설치하세요: pip install OpenDartReader")
    if not config.DART_API_KEY:
        raise ValueError("DART_API_KEY가 설정되지 않았습니다. GitHub Secrets에 등록하세요.")
    return OpenDartReader.OpenDartReader(config.DART_API_KEY)


def get_dart_financials(ticker: str, year: int) -> dict:
    """
    DART 단일 종목 연간 재무제표 수집.
    연결재무제표(CFS) 우선, 없으면 별도재무제표(OFS) 사용.

    Returns: dict {year, ticker, name, fs_type, 매출액, 영업이익, ...}
    """
    dart = _get_dart()
    time.sleep(random.uniform(*config.DELAY_API))

    try:
        result = {"year": year, "ticker": ticker}

        for fs_div in ["CFS", "OFS"]:
            df = dart.finstate(ticker, year, reprt_code="11011", fs_div=fs_div)
            if df is not None and not df.empty:
                result["fs_type"] = fs_div
                break
        else:
            logger.debug(f"[DART] {ticker} {year} 재무제표 없음")
            return {}

        # 종목명
        try:
            result["name"] = dart.corp_name(ticker) or ""
        except Exception:
            result["name"] = ""

        # 계정 과목 매핑
        account_map = {
            "매출액":       ["매출액", "영업수익", "수익(매출액)"],
            "매출원가":     ["매출원가", "영업비용"],
            "영업이익":     ["영업이익", "영업이익(손실)"],
            "당기순이익":   ["당기순이익", "당기순이익(손실)", "분기순이익"],
            "자산총계":     ["자산총계"],
            "부채총계":     ["부채총계"],
            "자본총계":     ["자본총계"],
            "영업현금흐름": ["영업활동현금흐름", "영업활동으로인한현금흐름"],
            "매출총이익":   ["매출총이익"],
        }

        df["account_nm"] = df["account_nm"].str.strip()

        for col, keywords in account_map.items():
            for kw in keywords:
                row = df[df["account_nm"] == kw]
                if not row.empty:
                    val_str = str(row.iloc[0].get("thstrm_amount", "0") or "0").replace(",", "")
                    try:
                        result[col] = float(val_str)
                    except ValueError:
                        result[col] = None
                    break

        # 매출총이익 = 매출액 - 매출원가 (직접 수록이 없을 경우 계산)
        if "매출총이익" not in result and "매출액" in result and "매출원가" in result:
            r, c = result.get("매출액"), result.get("매출원가")
            if r is not None and c is not None:
                result["매출총이익"] = r - c

        return result

    except Exception as e:
        logger.error(f"[DART] {ticker} {year} 수집 실패: {e}")
        return {}


def get_all_dart_financials(tickers: list[str], year: int,
                             dry_run: bool = False) -> pd.DataFrame:
    """전종목 재무제표 배치 수집."""
    try:
        from tqdm import tqdm
        ticker_iter = tqdm(tickers, desc=f"DART {year}년 재무제표")
    except ImportError:
        ticker_iter = tickers

    rows = []
    for i, ticker in enumerate(ticker_iter):
        if dry_run:
            logger.info(f"[DryRun] DART {ticker} {year}")
            continue
        row = get_dart_financials(ticker, year)
        if row:
            rows.append(row)
        if (i + 1) % 100 == 0:
            logger.info(f"[DART] {i + 1}/{len(tickers)} 완료")

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# [4] CompanyGuide (FnGuide) — 재무비율 크롤링
# ══════════════════════════════════════════════════════════════════════════════

_CG_BASE = "http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
_CG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "http://comp.fnguide.com/",
}


def get_company_guide(ticker: str) -> dict:
    """
    CompanyGuide에서 재무비율 수집.
    Returns: {ticker, ROE, ROA, 영업이익률, 순이익률, 부채비율, EV_EBITDA}
    """
    time.sleep(random.uniform(*config.DELAY_TICKER))
    pno = f"A{ticker}"
    params = {"pGubun": "1", "pNm": "SummaryIR", "pSelSCode": pno,
              "pYear": "", "pRpt_tp": "A"}
    result = {"ticker": ticker}

    for attempt in range(config.MAX_RETRY):
        try:
            resp = requests.get(_CG_BASE, params=params, headers=_CG_HEADERS, timeout=10)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")
            result.update(_parse_cg_profitability(soup))
            result.update(_parse_cg_stability(soup))
            result["EV_EBITDA"] = _get_cg_ev_ebitda(ticker)
            return result
        except requests.RequestException as e:
            logger.debug(f"[CG] {ticker} 시도 {attempt + 1} 실패: {e}")
            time.sleep(config.BACKOFF_BASE ** attempt)

    return result


def _parse_cg_profitability(soup: BeautifulSoup) -> dict:
    result = {}
    try:
        for table in soup.find_all("table"):
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            if "ROE" not in headers and "영업이익률" not in headers:
                continue
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(strip=True)
                val   = _safe_float(cells[-1].get_text(strip=True))
                if   "ROE"    in label: result["ROE"]    = val
                elif "ROA"    in label: result["ROA"]    = val
                elif "영업이익률" in label: result["영업이익률"] = val
                elif "순이익률" in label or "당기순이익률" in label: result["순이익률"] = val
    except Exception as e:
        logger.debug(f"[CG] 수익성 파싱 오류: {e}")
    return result


def _parse_cg_stability(soup: BeautifulSoup) -> dict:
    result = {}
    try:
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                if "부채비율" in cells[0].get_text(strip=True):
                    result["부채비율"] = _safe_float(cells[-1].get_text(strip=True))
                    return result
    except Exception as e:
        logger.debug(f"[CG] 안정성 파싱 오류: {e}")
    return result


def _get_cg_ev_ebitda(ticker: str) -> Optional[float]:
    try:
        time.sleep(0.3)
        url = "http://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp"
        resp = requests.get(url, params={"pGubun": "1", "pNm": "Invest",
                                          "pSelSCode": f"A{ticker}"},
                            headers=_CG_HEADERS, timeout=10)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "lxml")
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2 and "EV/EBITDA" in cells[0].get_text(strip=True):
                    return _safe_float(cells[-1].get_text(strip=True))
    except Exception as e:
        logger.debug(f"[CG] EV/EBITDA 수집 실패 {ticker}: {e}")
    return None


def get_all_company_guide(tickers: list[str], year: int,
                           dry_run: bool = False) -> pd.DataFrame:
    """전종목 CompanyGuide 배치 수집."""
    try:
        from tqdm import tqdm
        ticker_iter = tqdm(tickers, desc=f"CompanyGuide {year}년")
    except ImportError:
        ticker_iter = tickers

    rows = []
    for i, ticker in enumerate(ticker_iter):
        if dry_run:
            logger.info(f"[DryRun] CG {ticker} {year}")
            continue
        row = get_company_guide(ticker)
        row["year"] = year
        rows.append(row)
        if (i + 1) % 50 == 0:
            logger.info(f"[CG] {i + 1}/{len(tickers)} 완료")

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
