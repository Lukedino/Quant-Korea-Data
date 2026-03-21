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
    특정 연도의 12개월 시장 스냅샷 + 일별 주가 수집.

    Args:
        year:         수집 연도
        skip_if_done: True면 이미 완료된 월 건너뜀
        skip_prices:  True면 일별 OHLCV 수집 생략
        dry_run:      True면 실제 저장 없이 플로우만 확인
        upload_drive: True면 각 월 완료 후 Drive 업로드
    """
    logger.info(f"[Historical] ===== {year}년 수집 시작 =====")

    for month in range(1, 13):
        ym = _yyyymm(year, month)
        end_date = _month_end_date(year, month)

        # 미래 날짜 스킵
        if end_date > datetime.today().strftime("%Y%m%d"):
            logger.info(f"[Historical] {ym} 미래 날짜 → 스킵")
            continue

        # ── 시장 스냅샷 ────────────────────────────────────────────────────
        if skip_if_done and progress.is_done("market", ym):
            logger.debug(f"[Historical] market/{ym} 이미 완료 → 스킵")
        else:
            progress.mark_in_progress("market", ym)
            logger.info(f"[Historical] [{ym}] 시장 스냅샷 수집 중... (기준일: {end_date})")

            if not dry_run:
                df = collector.get_market_snapshot(end_date)
                if not df.empty:
                    storage.save_market(df, ym)
                    progress.mark_done("market", ym)

                    if upload_drive:
                        _upload_file("market", ym)
                else:
                    logger.warning(f"[Historical] {ym} 시장 스냅샷 수집 실패")
            else:
                logger.info(f"[DryRun] market/{ym} 수집 시뮬레이션")
                progress.mark_done("market", ym)

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

    logger.info(f"[Historical] ===== {year}년 수집 완료 =====")


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
    dry_run: bool = False,
    upload_drive: bool = False,
):
    """
    연도 범위 일괄 수집. start_year ~ end_year (포함).

    수집 순서:
      1. 시장 스냅샷 + 일별 주가 (연도별)
      2. DART 재무제표 (연도별)
    """
    logger.info(f"[Historical] 범위 수집: {start_year}년 ~ {end_year}년")

    for year in range(start_year, end_year + 1):
        collect_year(
            year,
            skip_if_done=skip_if_done,
            skip_prices=skip_prices,
            dry_run=dry_run,
            upload_drive=upload_drive,
        )
        if not skip_financials:
            collect_financials_year(
                year,
                skip_if_done=skip_if_done,
                dry_run=dry_run,
                upload_drive=upload_drive,
            )

    logger.info(f"[Historical] 범위 수집 완료: {start_year}~{end_year}")
    progress.print_summary()
    storage.print_local_summary()


# ── Drive 업로드 헬퍼 ─────────────────────────────────────────────────────────

def _upload_file(data_type: str, key: str):
    """수집 완료된 파일을 Drive에 업로드."""
    from pathlib import Path
    import config as cfg

    if not cfg.GDRIVE_FOLDER_ID or not Path(cfg.GDRIVE_CREDS_PATH).exists():
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
