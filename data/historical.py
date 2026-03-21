"""
data/historical.py — 연도/기간 단위 일괄 수집 조율

수집 단위:
  collect_year(year)           → 해당 연도 12개월 시장 스냅샷 + 일별 주가
  collect_financials_year(year)→ DART + CompanyGuide 연간 재무제표
  collect_range(start, end)    → 연도 범위 순회

체크포인트:
  - progress.is_done() 로 완료 여부 확인 → 완료된 월/연도 스킵
  - 수집 완료 시 progress.mark_done() 기록
"""

import logging
from datetime import datetime, timedelta

import config
from data import collector, storage, progress

logger = logging.getLogger(__name__)


# ── 유틸리티 ──────────────────────────────────────────────────────────────────

def _month_end_date(year: int, month: int) -> str:
    """해당 월 마지막 날 (토/일이면 전 금요일) YYYYMMDD 반환."""
    if month == 12:
        last = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = datetime(year, month + 1, 1) - timedelta(days=1)
    while last.weekday() >= 5:  # 토=5, 일=6
        last -= timedelta(days=1)
    return last.strftime("%Y%m%d")


def _yyyymm(year: int, month: int) -> str:
    return f"{year:04d}{month:02d}"


# ── 메인 수집 함수 ────────────────────────────────────────────────────────────

def collect_year(
    year: int,
    skip_if_done: bool = True,
    skip_prices: bool = False,
    dry_run: bool = False,
    upload_drive: bool = False,
):
    """
    특정 연도의 12개월 일별 주가(yfinance) 수집.

    시장 스냅샷(PER/PBR)은 collect_market_range()로 일괄 수집.

    Args:
        year:         수집 연도
        skip_if_done: 이미 완료된 월 건너뜀
        skip_prices:  일별 OHLCV 수집 생략
        dry_run:      실제 저장 없이 플로우만 확인
        upload_drive: 각 월 완료 후 Drive 업로드
    """
    logger.info(f"[Historical] ===== {year}년 가격 수집 시작 =====")

    for month in range(1, 13):
        ym = _yyyymm(year, month)
        end_date = _month_end_date(year, month)

        # 미래 날짜 스킵
        if end_date > datetime.today().strftime("%Y%m%d"):
            logger.info(f"[Historical] {ym} 미래 날짜 → 스킵")
            continue

        # ── 일별 주가 ──────────────────────────────────────────────────────
        if skip_prices:
            logger.debug(f"[Historical] prices/{ym} 건너뜀 (--skip-prices)")
        elif skip_if_done and progress.is_done("prices", ym):
            logger.debug(f"[Historical] prices/{ym} 이미 완료 → 스킵")
        else:
            progress.mark_in_progress("prices", ym)
            logger.info(f"[Historical] [{ym}] 일별 주가 수집 중...")

            if not dry_run:
                df = collector.get_daily_prices_month(ym)
                if not df.empty:
                    storage.save_prices(df, ym)
                    progress.mark_done("prices", ym)
                    if upload_drive:
                        _upload_file("prices", ym)
                else:
                    logger.warning(f"[Historical] {ym} 일별 주가 수집 실패")
            else:
                logger.info(f"[DryRun] prices/{ym} 수집 시뮬레이션")
                progress.mark_done("prices", ym)

    logger.info(f"[Historical] ===== {year}년 가격 수집 완료 =====")


def collect_market_range(
    start_year: int,
    end_year: int,
    skip_if_done: bool = True,
    dry_run: bool = False,
    upload_drive: bool = False,
):
    """
    전체 기간 시장 스냅샷(PER/PBR/EPS/BPS/DIV) 일괄 수집.

    pykrx 개별종목 API(get_market_fundamental)를 사용해 전종목 × 1회 호출.
    GitHub Actions Azure IP에서 차단 없음 — 약 10분 소요.

    수집 후 월별 말일 기준으로 market/YYYYMM.parquet 저장.
    """
    today_str = datetime.today().strftime("%Y%m%d")

    # 이미 완료된 월 확인
    months_needed = [
        (y, m)
        for y in range(start_year, end_year + 1)
        for m in range(1, 13)
        if _month_end_date(y, m) <= today_str
        and not (skip_if_done and progress.is_done("market", _yyyymm(y, m)))
    ]

    if not months_needed:
        logger.info("[Historical] market 모두 완료 → 스킵")
        return

    if dry_run:
        for y, m in months_needed:
            ym = _yyyymm(y, m)
            logger.info(f"[DryRun] market/{ym} 수집 시뮬레이션")
            progress.mark_done("market", ym)
        return

    start_date = f"{start_year}0101"
    end_date   = f"{end_year}1231"
    logger.info(
        f"[Historical] 펀더멘탈 일괄 수집: {start_date}~{end_date} "
        f"({len(months_needed)}개월 저장 예정)"
    )

    fund_df = collector.get_fundamentals_range(start_date, end_date)
    if fund_df.empty:
        logger.warning("[Historical] 펀더멘탈 수집 실패 — market 스킵")
        return

    # 월별 말일 기준으로 재구성하여 저장
    for y, m in months_needed:
        ym      = _yyyymm(y, m)
        eom     = _month_end_date(y, m)          # YYYYMMDD (영업일 기준)
        ym_str  = f"{y:04d}{m:02d}"              # YYYYMM prefix

        # 해당 월 데이터 중 말일에 가장 가까운 날짜 선택
        month_df = fund_df[
            (fund_df["date"] >= f"{ym_str}01") &
            (fund_df["date"] <= eom)
        ]
        if month_df.empty:
            logger.warning(f"[Historical] market/{ym} 데이터 없음 → 스킵")
            continue

        last_date = month_df["date"].max()
        snap = month_df[month_df["date"] == last_date].copy()
        snap["date"] = ym  # 월키로 통일

        storage.save_market(snap, ym)
        progress.mark_done("market", ym)
        if upload_drive:
            _upload_file("market", ym)
        logger.debug(f"[Historical] market/{ym} 저장 완료 ({len(snap)}종목)")

    logger.info(f"[Historical] 펀더멘탈 저장 완료: {len(months_needed)}개월")


def collect_financials_year(
    year: int,
    skip_if_done: bool = True,
    dry_run: bool = False,
    upload_drive: bool = False,
):
    """
    DART + CompanyGuide 연간 재무제표 수집.

    Args:
        year:         수집 연도 (사업보고서 기준)
        skip_if_done: 이미 완료된 연도 스킵
        dry_run:      실제 저장 없이 시뮬레이션
        upload_drive: 완료 후 Drive 업로드
    """
    key = str(year)

    if skip_if_done and progress.is_done("financials", key):
        logger.info(f"[Historical] financials/{year} 이미 완료 → 스킵")
        return

    # 미래 연도 스킵 (당해년도 사업보고서는 다음해 4~5월에 확정)
    if year >= datetime.today().year:
        logger.info(f"[Historical] financials/{year} 미확정 연도 → 스킵")
        return

    progress.mark_in_progress("financials", key)
    logger.info(f"[Historical] {year}년 재무제표 수집 시작...")

    if dry_run:
        logger.info(f"[DryRun] financials/{year} 수집 시뮬레이션")
        progress.mark_done("financials", key)
        return

    # 유니버스 조회 (현재 시점 기준)
    today = datetime.today().strftime("%Y%m%d")
    try:
        tickers = collector.get_universe(today)
        logger.info(f"[Historical] 유니버스: {len(tickers)}종목")
    except Exception as e:
        logger.error(f"[Historical] 종목 목록 조회 실패: {e}")
        return

    # DART 재무제표
    logger.info(f"[Historical] DART {year}년 수집 중...")
    fin_df = collector.get_all_dart_financials(tickers, year, dry_run=dry_run)
    if not fin_df.empty:
        storage.save_financials(fin_df, year)
        logger.info(f"[Historical] DART {year} 저장 완료: {len(fin_df)}종목")

    progress.mark_done("financials", key)
    logger.info(f"[Historical] {year}년 재무제표 수집 완료")

    if upload_drive:
        _upload_file("financials", key)


def collect_range(
    start_year: int,
    end_year: int,
    skip_if_done: bool = True,
    skip_prices: bool = False,
    skip_financials: bool = False,
    skip_market: bool = False,
    dry_run: bool = False,
    upload_drive: bool = False,
):
    """
    연도 범위 일괄 수집. start_year ~ end_year (포함).

    수집 순서:
      Phase 1. 일별 주가  (yfinance, 월별)
      Phase 2. 시장 스냅샷 PER/PBR (pykrx 개별 API, 전기간 1회)
      Phase 3. DART 재무제표 (연도별)
    """
    total = end_year - start_year + 1
    logger.info(f"[Historical] 범위 수집 시작: {start_year}~{end_year}년 ({total}년치)")

    # Phase 1: 일별 주가
    if not skip_prices:
        logger.info(f"[Historical] Phase 1: 일별 주가 수집 ({start_year}~{end_year})")
        for i, year in enumerate(range(start_year, end_year + 1), 1):
            logger.info(f"[Historical] ── {year}년 가격 ({i}/{total}) ──")
            collect_year(
                year,
                skip_if_done=skip_if_done,
                skip_prices=False,
                dry_run=dry_run,
                upload_drive=upload_drive,
            )

    # Phase 2: 시장 스냅샷 (PER/PBR) — pykrx 개별 API 일괄
    if not skip_market:
        logger.info(f"[Historical] Phase 2: 시장 스냅샷(펀더멘탈) 수집 ({start_year}~{end_year})")
        collect_market_range(
            start_year=start_year,
            end_year=end_year,
            skip_if_done=skip_if_done,
            dry_run=dry_run,
            upload_drive=upload_drive,
        )

    # Phase 3: DART 재무제표
    if not skip_financials:
        logger.info(f"[Historical] Phase 3: DART 재무제표 수집 ({start_year}~{end_year})")
        for i, year in enumerate(range(start_year, end_year + 1), 1):
            logger.info(f"[Historical] ── {year}년 재무제표 ({i}/{total}) ──")
            collect_financials_year(
                year,
                skip_if_done=skip_if_done,
                dry_run=dry_run,
                upload_drive=upload_drive,
            )

    logger.info(f"[Historical] 범위 수집 완료: {start_year}~{end_year}년")
    progress.print_summary()
    storage.print_local_summary()


# ── Drive 업로드 헬퍼 ─────────────────────────────────────────────────────────

def _upload_file(data_type: str, key: str):
    """수집 완료된 파일을 Drive에 업로드."""
    from pathlib import Path
    import config as cfg

    has_token = cfg.GDRIVE_TOKEN_PATH and Path(cfg.GDRIVE_TOKEN_PATH).exists()
    has_sa    = Path(cfg.GDRIVE_CREDS_PATH).exists()
    if not cfg.GDRIVE_FOLDER_ID or (not has_token and not has_sa):
        logger.debug("[Historical] Drive 자격증명 없음 → 업로드 건너뜀")
        return

    try:
        from data.drive_uploader import DriveUploader
        uploader = DriveUploader()

        if data_type == "market":
            local = str(Path(cfg.LOCAL_DATA_DIR) / "market" / f"{key}.parquet")
            remote = cfg.DRIVE_PATHS["market"]
        elif data_type == "prices":
            local = str(Path(cfg.LOCAL_DATA_DIR) / "prices" / f"{key}.parquet")
            remote = cfg.DRIVE_PATHS["prices"]
        elif data_type == "financials":
            local = str(Path(cfg.LOCAL_DATA_DIR) / "financials" / f"{key}.parquet")
            remote = cfg.DRIVE_PATHS["financials"]
        else:
            return

        if Path(local).exists():
            uploader.upload(local, remote)
            logger.info(f"[Historical] Drive 업로드 완료: {data_type}/{key}")
    except Exception as e:
        logger.warning(f"[Historical] Drive 업로드 실패 {data_type}/{key}: {e}")
