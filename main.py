"""
main.py — 수집 전용 CLI 진입점

모드:
  daily      : 오늘 기준 시장 스냅샷 + 이번 달 일별 주가 수집
  bootstrap  : 특정 연도 과거 데이터 일괄 수집 (체크포인트 기반)

사용 예:
  # 오늘 데이터 수집 후 Drive 업로드
  python main.py --mode daily --upload-drive

  # 2025년(1년 전) 데이터 수집
  python main.py --mode bootstrap --years-ago 1 --upload-drive

  # 특정 연도 직접 지정
  python main.py --mode bootstrap --year 2022 --skip-prices

  # 실제 저장 없이 테스트
  python main.py --mode bootstrap --years-ago 1 --dry-run
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# ── 로깅 설정 ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("collection.log", encoding="utf-8"),
    ],
)
# pykrx 내부 노이즈 억제
logging.getLogger("pykrx").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("google").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# daily 모드
# ══════════════════════════════════════════════════════════════════════════════

def run_daily(args):
    """
    오늘 기준 일별 수집:
    - 오늘 날짜 시장 스냅샷 수집
    - 이번 달 일별 주가 수집
    - Drive 업로드 (--upload-drive 시)
    """
    from data import collector, storage, progress

    today    = datetime.today()
    yyyymm   = today.strftime("%Y%m")
    date_str = collector.get_last_business_day()

    logger.info(f"[Daily] 기준일: {date_str} / 대상 월: {yyyymm}")

    # ── 시장 스냅샷 ────────────────────────────────────────────────────────
    if args.dry_run:
        logger.info(f"[DryRun] market/{yyyymm} 수집 시뮬레이션")
    else:
        logger.info(f"[Daily] 시장 스냅샷 수집 중...")
        df = collector.get_market_snapshot(date_str)
        if not df.empty:
            storage.save_market(df, yyyymm)
            progress.mark_done("market", yyyymm)
            logger.info(f"[Daily] 시장 스냅샷 저장 완료: {len(df)}종목")
        else:
            logger.warning("[Daily] 시장 스냅샷 수집 실패")

    # ── 이번 달 일별 주가 ──────────────────────────────────────────────────
    if not args.skip_prices:
        if args.dry_run:
            logger.info(f"[DryRun] prices/{yyyymm} 수집 시뮬레이션")
        else:
            logger.info(f"[Daily] 일별 주가 수집 중 ({yyyymm})...")
            df = collector.get_daily_prices_month(yyyymm)
            if not df.empty:
                storage.save_prices(df, yyyymm)
                progress.mark_done("prices", yyyymm)
                logger.info(f"[Daily] 일별 주가 저장 완료: {len(df):,}건")
            else:
                logger.warning("[Daily] 일별 주가 수집 실패")

    # ── Drive 업로드 ───────────────────────────────────────────────────────
    if args.upload_drive and not args.dry_run:
        _upload_all()

    progress.print_summary()
    storage.print_local_summary()
    logger.info("[Daily] 완료")


# ══════════════════════════════════════════════════════════════════════════════
# bootstrap 모드
# ══════════════════════════════════════════════════════════════════════════════

def run_bootstrap(args):
    """
    과거 데이터 일괄 수집.

    단일 연도:
      --years-ago N  → 현재년도 - N 연도 1개
      --year YYYY    → 직접 연도 지정

    범위 수집:
      --years-range N   → 최근 N년치 (current-N ~ current-1)
      --year-start YYYY → YYYY ~ current-1 전체 (max 모드)
    """
    from data import historical, progress

    current_year = datetime.today().year
    skip = not args.force

    # ── 범위 수집 (years-range / year-start) ──────────────────────────────
    if args.year_start or args.years_range:
        if args.year_start:
            start_year = args.year_start
            label = f"{start_year}년~{current_year - 1}년 (최대치)"
        else:
            start_year = current_year - args.years_range
            label = f"{start_year}년~{current_year - 1}년 ({args.years_range}년치)"

        end_year = current_year - 1

        logger.info(
            f"[Bootstrap] 범위 수집: {label} | "
            f"skip_prices={args.skip_prices}, skip_financials={args.skip_financials}, "
            f"dry_run={args.dry_run}"
        )

        historical.collect_range(
            start_year=start_year,
            end_year=end_year,
            skip_if_done=skip,
            skip_prices=args.skip_prices,
            skip_financials=args.skip_financials,
            dry_run=args.dry_run,
            upload_drive=args.upload_drive,
        )

    # ── 단일 연도 수집 (years-ago / year) ─────────────────────────────────
    elif args.year or args.years_ago:
        target_year = args.year if args.year else current_year - args.years_ago

        logger.info(
            f"[Bootstrap] 단일 연도: {target_year}년 | "
            f"skip_prices={args.skip_prices}, skip_financials={args.skip_financials}, "
            f"dry_run={args.dry_run}"
        )

        historical.collect_year(
            year=target_year,
            skip_if_done=skip,
            skip_prices=args.skip_prices,
            dry_run=args.dry_run,
            upload_drive=args.upload_drive,
        )
        if not args.skip_financials:
            historical.collect_financials_year(
                year=target_year,
                skip_if_done=skip,
                dry_run=args.dry_run,
                upload_drive=args.upload_drive,
            )

    else:
        logger.error("bootstrap 모드에는 --years-ago / --year / --years-range / --year-start 중 하나가 필요합니다.")
        sys.exit(1)

    # 최종 Drive 전체 업로드
    if args.upload_drive and not args.dry_run:
        _upload_all()

    progress.print_summary()
    storage.print_local_summary()
    logger.info("[Bootstrap] 완료")


# ══════════════════════════════════════════════════════════════════════════════
# Drive 업로드 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _upload_all():
    """로컬 data/ 전체를 Drive에 동기화."""
    import config
    if not config.GDRIVE_FOLDER_ID:
        logger.warning("[Upload] GDRIVE_FOLDER_ID 미설정 → 업로드 건너뜀")
        return
    if not Path(config.GDRIVE_CREDS_PATH).exists():
        logger.warning(f"[Upload] 자격증명 없음 ({config.GDRIVE_CREDS_PATH}) → 건너뜀")
        return

    try:
        from data.drive_uploader import DriveUploader
        DriveUploader().sync_all_local()
    except Exception as e:
        logger.error(f"[Upload] Drive 동기화 실패: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="한국 주식 퀀트 데이터 수집기 (수집 전용)",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--mode", choices=["daily", "bootstrap"], required=True,
        help="daily: 오늘 수집 / bootstrap: 과거 연도 일괄 수집",
    )

    # bootstrap 전용
    parser.add_argument(
        "--years-ago", type=int, metavar="N",
        help="bootstrap: 현재 기준 N년 전 단일 연도 수집 (예: 1 → 2025년)",
    )
    parser.add_argument(
        "--year", type=int, metavar="YYYY",
        help="bootstrap: 직접 단일 연도 지정",
    )
    parser.add_argument(
        "--years-range", type=int, metavar="N",
        help="bootstrap: 최근 N년치 범위 수집 (예: 3 → 2023~2025년)",
    )
    parser.add_argument(
        "--year-start", type=int, metavar="YYYY",
        help="bootstrap: YYYY년부터 현재까지 전체 수집 (최대치 모드, 예: 2010)",
    )

    # 수집 제어
    parser.add_argument(
        "--skip-prices", action="store_true",
        help="일별 주가 수집 생략 (시장 스냅샷만 수집)",
    )
    parser.add_argument(
        "--skip-financials", action="store_true",
        help="재무제표 수집 생략 (bootstrap 시)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="체크포인트 무시 → 이미 완료된 월도 재수집",
    )

    # Drive & 기타
    parser.add_argument(
        "--upload-drive", action="store_true",
        help="수집 완료 후 Google Drive에 업로드",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="실제 저장 없이 수집 플로우만 테스트",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="현재 수집 현황만 출력 후 종료",
    )

    args = parser.parse_args()

    # 현황 출력 모드
    if args.status:
        from data import progress, storage
        progress.print_summary()
        storage.print_local_summary()
        return

    logger.info("=" * 60)
    logger.info(f"  Quant-Korea-Data 수집기 시작")
    logger.info(f"  모드: {args.mode} | DryRun: {args.dry_run}")
    logger.info("=" * 60)

    if args.mode == "daily":
        run_daily(args)
    elif args.mode == "bootstrap":
        run_bootstrap(args)


if __name__ == "__main__":
    # 로컬 임포트 경로 보장
    import os
    sys.path.insert(0, os.path.dirname(__file__))

    # storage를 main에서 직접 참조하기 위한 임포트
    from data import storage

    main()
