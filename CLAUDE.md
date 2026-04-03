# Daily-Market-Data-Crawling

## 프로젝트 개요
GitHub Actions로 매일 자동 실행되는 시장 데이터 크롤러.
US 주식/ETF, 크립토, KR(한국) 시장 OHLC + 재무데이터를 수집해 연도별 Parquet으로 Google Drive에 저장.

- GitHub: https://github.com/Lukedino/Daily-Market-Data-Crawling
- 브랜치: main

---

## Google Drive 구조

### 루트 폴더
- `GDRIVE_OHLC_FOLDER_ID` = `[Database] Market Crawling Data` 폴더 ID
- GCP 프로젝트: `seraphic-jet-489008-b4`
- Service Account: `stock-crawler-bot@seraphic-jet-489008-b4.iam.gserviceaccount.com`

### 서브폴더 구조
```
[Database] Market Crawling Data/   ← GDRIVE_OHLC_FOLDER_ID
    ├─ kr/          ← KR OHLC + 시총 (marcap 스키마, 연도별 parquet)
    │   ├─ marcap-2025.parquet
    │   ├─ marcap-2026.parquet
    │   └─ marcap-2027~2030.parquet  (빈 플레이스홀더, 수동 업로드)
    ├─ us/          ← US 주식/ETF OHLC (연도별 parquet)
    │   └─ us_YYYY.parquet
    ├─ crypto/      ← 크립토 OHLC (연도별 parquet)
    │   └─ crypto_YYYY.parquet
    └─ _meta/       ← DB 상태 메타 (db_status.json)
```

> ⚠️ Service Account는 신규 파일 생성 불가 (storageQuotaExceeded). 신규 연도 파일은 수동 업로드 후 SA가 update만 가능.
> 2027~2030년 빈 플레이스홀더 파일은 이미 생성 완료 → Drive에 수동 업로드 필요.

---

## GitHub Secrets

| Secret | 용도 |
|--------|------|
| `GDRIVE_OHLC_FOLDER_ID` | `[Database] Market Crawling Data` 폴더 ID |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | GCP Service Account JSON 키 |
| `DART_API_KEY` | DART 재무데이터 API 키 |

---

## GitHub Actions 워크플로우

| 파일 | 스케줄 | 역할 |
|------|--------|------|
| `kr-daily.yml` | 평일 07:30 UTC (16:30 KST) | KR 일별 스냅샷 수집 + 자동 갭 보정 |
| `kr-backfill.yml` | workflow_dispatch | KR 누락 구간 과거 수집 |
| `ohlc-daily.yml` | 자동 | US/Crypto OHLC 일별 수집 |
| `ohlc-backfill.yml` | workflow_dispatch | US/Crypto OHLC 과거 수집 |
| `financials-update.yml` | 자동 | US 재무데이터 수집 |

---

## 실행 방법 (main.py)

```bash
# KR 당일 스냅샷
python main.py --mode kr-daily --upload-drive

# KR 과거 백필
python main.py --mode kr-backfill --start-date 2026-01-02 --end-date 2026-03-31 --upload-drive

# 드라이런 (저장 없음)
python main.py --mode kr-daily --dry-run
```

---

## 데이터 구조 및 주요 모듈

### KR 시장 (marcap 스키마)

**스키마 컬럼:**
```
Code | Name | Close | Dept | ChangeCode | Changes | ChangesRatio |
Volume | Amount | Open | High | Low | Marcap | Stocks |
Market | MarketId | Rank | Date
```

**수집 전략:**
- `[daily]` FinanceDataReader StockListing × KOSPI + KOSDAQ + KONEX
  - 당일 스냅샷: OHLCV + Marcap + Rank + Market 포함
  - Rank = 시장 내 시총 기준 내림차순
- `[backfill]` yfinance .KS/.KQ 배치 수집 (100종목씩)
  - 과거 OHLCV만 (Marcap/Rank = NaN)
  - pykrx 전종목 엔드포인트는 GHA 환경에서 차단됨 → yfinance 우회

**주요 파일:**
- `data/kr_collector.py` — FDR daily + yfinance backfill 수집 로직
- `data/kr_db.py` — Parquet 저장/로드, Drive 업로드/다운로드
- `scripts/verify_kr.py` — DB 현황 검증 + 누락 구간 감지 + 자동 보정

### US / Crypto OHLC

- `data/ohlc_collector.py` — yfinance 기반 수집
- `data/ohlc_db.py` — 연도별 Parquet 관리

---

## 자동 갭 보정 (kr-daily)

`run_kr_daily()`는 매일 실행 시:
1. Drive에서 현재 연도 parquet 다운로드
2. `last_date` 확인 → 어제까지 누락된 영업일 계산
3. 누락 구간이 있으면 yfinance backfill 자동 실행
4. FDR StockListing으로 오늘 스냅샷 수집
5. Drive에 업로드

---

## 검증 스크립트

```bash
# 로컬 검증
python scripts/verify_kr.py

# Drive에서 다운로드 후 검증
python scripts/verify_kr.py --drive

# 누락 자동 보정
python scripts/verify_kr.py --drive --fix
```

---

## 로컬 파일 경로

- KR parquet: `data/local/ohlc_db/kr/marcap-YYYY.parquet`
- US parquet: `data/local/ohlc_db/us/us_YYYY.parquet`
- Crypto parquet: `data/local/ohlc_db/crypto/crypto_YYYY.parquet`

---

## 주요 이력

| 날짜 | 변경 내용 |
|------|---------|
| 2026-03-25 | 레포명 변경: Quant-Korea-Data → Daily-Market-Data-Crawling |
| 2026-04-03 | KR 시장 수집 추가 (kr_collector.py, kr_db.py, kr-daily.yml, kr-backfill.yml) |
| 2026-04-03 | Drive 폴더 marcap/ → kr/ 로 표준화 |
| 2026-04-03 | verify_kr.py 추가 (누락 구간 감지 + 자동 보정) |
| 2026-04-03 | 자동 갭 보정 로직 main.py run_kr_daily()에 추가 |
