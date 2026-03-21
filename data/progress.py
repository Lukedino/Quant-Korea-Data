"""
data/progress.py — 수집 진행상황 체크포인트 관리

Drive ↔ 로컬 동기화로 재시작 시 이어서 수집 가능.
collection_status.json 구조:
{
  "market":     {"202501": "done", "202502": "in_progress"},
  "financials": {"2025": "done"},
  "prices":     {"202501": "done"},
  "last_updated": "2026-03-21T09:30:00"
}
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import config

logger = logging.getLogger(__name__)

STATUS_FILENAME  = "collection_status.json"
LOCAL_STATUS_DIR = Path(config.LOCAL_DATA_DIR) / "progress"
LOCAL_STATUS_PATH = LOCAL_STATUS_DIR / STATUS_FILENAME

_cache: dict | None = None  # 세션 내 캐시 (Drive 반복 호출 방지)


def _empty_status() -> dict:
    return {
        "market":     {},
        "financials": {},
        "prices":     {},
        "last_updated": "",
    }


def load_status(force_drive: bool = False) -> dict:
    """
    status 로드. 우선순위:
    1. 세션 캐시 (force_drive=False)
    2. Drive 다운로드
    3. 로컬 파일
    4. 빈 dict 반환
    """
    global _cache
    if _cache is not None and not force_drive:
        return _cache

    LOCAL_STATUS_DIR.mkdir(parents=True, exist_ok=True)

    # Drive에서 최신 status 다운로드 시도
    if config.GDRIVE_FOLDER_ID and Path(config.GDRIVE_CREDS_PATH).exists():
        try:
            from data.drive_uploader import DriveUploader
            uploader = DriveUploader()
            uploader.download(
                remote_subfolder=config.DRIVE_PATHS["progress"],
                filename=STATUS_FILENAME,
                local_path=str(LOCAL_STATUS_PATH),
            )
            logger.info("[Progress] Drive에서 status 동기화 완료")
        except Exception as e:
            logger.debug(f"[Progress] Drive 다운로드 실패 (로컬 사용): {e}")

    # 로컬 파일 읽기
    if LOCAL_STATUS_PATH.exists():
        try:
            with open(LOCAL_STATUS_PATH, "r", encoding="utf-8") as f:
                _cache = json.load(f)
            return _cache
        except Exception as e:
            logger.warning(f"[Progress] status 파일 읽기 실패: {e}")

    _cache = _empty_status()
    return _cache


def save_status(status: dict, upload: bool = True):
    """로컬 저장 + Drive 업로드"""
    global _cache
    LOCAL_STATUS_DIR.mkdir(parents=True, exist_ok=True)
    status["last_updated"] = datetime.now().isoformat()
    _cache = status

    with open(LOCAL_STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

    if upload and config.GDRIVE_FOLDER_ID and Path(config.GDRIVE_CREDS_PATH).exists():
        try:
            from data.drive_uploader import DriveUploader
            DriveUploader().upload(
                local_path=str(LOCAL_STATUS_PATH),
                remote_subfolder=config.DRIVE_PATHS["progress"],
            )
        except Exception as e:
            logger.warning(f"[Progress] Drive 업로드 실패: {e}")


def is_done(data_type: str, key: str) -> bool:
    """예: is_done("market", "202501") → True/False"""
    return load_status().get(data_type, {}).get(key) == "done"


def mark_done(data_type: str, key: str):
    status = load_status()
    status.setdefault(data_type, {})[key] = "done"
    save_status(status)
    logger.info(f"[Progress] {data_type}/{key} → done")


def mark_in_progress(data_type: str, key: str):
    status = load_status()
    status.setdefault(data_type, {})[key] = "in_progress"
    save_status(status, upload=False)  # in_progress는 Drive 업로드 불필요


def get_missing_months(data_type: str, start_yyyymm: str, end_yyyymm: str) -> list[str]:
    """
    start_yyyymm ~ end_yyyymm 중 아직 "done"이 아닌 월 리스트 반환.
    재실행 시 이어서 수집하는 데 사용.
    """
    done_set = {
        k for k, v in load_status().get(data_type, {}).items()
        if v == "done"
    }

    result = []
    year  = int(start_yyyymm[:4])
    month = int(start_yyyymm[4:6])
    ey    = int(end_yyyymm[:4])
    em    = int(end_yyyymm[4:6])

    while (year, month) <= (ey, em):
        yyyymm = f"{year:04d}{month:02d}"
        if yyyymm not in done_set:
            result.append(yyyymm)
        month += 1
        if month > 12:
            year += 1
            month = 1

    return result


def get_missing_years(data_type: str, start_year: int, end_year: int) -> list[int]:
    """재무제표 등 연 단위 미수집 연도 리스트 반환"""
    done_set = {
        k for k, v in load_status().get(data_type, {}).items()
        if v == "done"
    }
    return [y for y in range(start_year, end_year + 1) if str(y) not in done_set]


def print_summary():
    """현재 수집 현황 출력"""
    status = load_status()
    print("\n=== 수집 현황 ===")
    for dtype in ("market", "financials", "prices"):
        items = status.get(dtype, {})
        done  = sum(1 for v in items.values() if v == "done")
        total = len(items)
        print(f"  {dtype:12s}: {done}/{total} 완료")
    print(f"  업데이트: {status.get('last_updated', '없음')}")
    print("=" * 20)
