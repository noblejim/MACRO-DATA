# MACRO-DATA 프로젝트 개요

> 작성일: 2026-02-14
> 목적: 프로젝트 구조 및 의도 파악 (Claude 학습용 참조 문서)

---

## 1. 프로젝트 핵심 요약

**MACRO-DATA**는 미국 및 한국 거시경제 데이터를 매일 자동으로 수집·분석하여
투자자를 위한 **인터랙티브 Excel 대시보드**를 생성하는 자동화 파이프라인이다.

- **Repository**: https://github.com/noblejim/MACRO-DATA
- **Main Branch**: main
- **Secrets Required**: `FRED_API_KEY`, `FMP_API_KEY`, `BOK_API_KEY`, `KOSIS_API_KEY`
- **자동 실행 일정**: 매일 02:00 KST (17:00 UTC)

---

## 2. 폴더 구조

```
C:\Users\ZIANNI\Documents\MACRO-DATA\
├── .git/                          # Git 저장소 메타데이터
├── .github/
│   └── workflows/
│       ├── nightly.yml            # 야간 자동 파이프라인 (02:00 KST)
│       └── release.yml            # 릴리즈 패키징 & GitHub Release 생성
├── scripts/                       # Python 파이프라인 스크립트 (18개)
│   ├── fetch_prices_fmp.py        # FMP API → 주가 데이터
│   ├── fetch_macro_from_fred.py   # FRED API → 미국 거시 데이터
│   ├── fetch_macro_from_bok_kosis.py  # BOK/KOSIS → 한국 거시 데이터
│   ├── fetch_prices_fdr.py        # FinanceDataReader (KR 폴백)
│   ├── prefetch_fred_series.py    # FRED 시리즈 사전 캐싱
│   ├── backfill_events_from_fred.py   # FRED에서 이벤트 날짜 백필
│   ├── seed_events_from_links.py  # 이벤트 레코드 생성
│   ├── merge_macro_actuals.py     # 거시 지표 + 실제값 병합
│   ├── compute_additional_windows.py  # ±5/±10/±21일 수익률 계산
│   ├── compute_macro_impact.py    # OLS 회귀분석 (surprise_z → 섹터 수익률)
│   ├── compute_reaction_by_surprise_quantile.py  # 서프라이즈 크기별 분위 분석
│   ├── compute_partial_impact.py  # 부분 영향 분석 (순위 통제)
│   ├── join_cycles_into_reactions.py  # 사이클 메트릭 병합
│   ├── build_reaction_matrix.py   # 반응 매트릭스 (wide format)
│   ├── analyze_focus_events.py    # 상위/하위 이벤트 포커스 분석
│   ├── build_excel_dashboard_plus.py  # Excel 워크북 + 히트맵/컨트롤
│   ├── postprocess_add_overview_links.py  # 대시보드 개요 하이퍼링크 추가
│   ├── validate_us_data.py        # 데이터 품질 검증
│   └── utils_surprise.py          # 서프라이즈 계산 유틸리티
├── data/
│   ├── us/                        # 미국 시장 설정 데이터
│   │   ├── macro_sources.csv      # 58개 거시 이벤트 정의 (CPI, NFP, FOMC 등)
│   │   ├── tickers.csv            # 23개 미국 주식 티커 (8개 섹터)
│   │   ├── sector_benchmarks.csv  # 11개 섹터 ETF 벤치마크
│   │   └── macro_regimes.csv      # 시장 레짐 정의 (Expansion, Slowdown 등)
│   ├── kr/                        # 한국 시장 설정 데이터
│   │   ├── macro_sources.csv      # 16개 한국 경제 지표
│   │   ├── macro_events.csv       # 한국 이벤트 데이터
│   │   ├── macro_regimes.csv      # 한국 레짐 정의
│   │   ├── tickers.csv            # 한국 주식 티커
│   │   ├── sector_benchmarks.csv  # 한국 섹터 벤치마크
│   │   └── ecos_overrides.csv     # BOK ECOS API 오버라이드
│   └── out/
│       └── logs/
│           └── fmp_fetch_log.csv  # FMP 데이터 수집 로그
├── out/                           # 스크립트 출력 디렉토리
│   ├── us/                        # 미국 분석 결과
│   └── kr/                        # 한국 분석 결과
├── dashboards/                    # Excel 대시보드 출력 폴더
├── docs/
│   ├── OPERATIONS_RUNBOOK.md      # 운영 절차서 (일상 운영 가이드)
│   └── SETUP_CI.md                # GitHub Actions 설정 가이드
├── 경제 리포트/                    # 참조용 경제 리포트 PDF (~500개+)
│   ├── 2025 전망/                 # 2025년 전망 보고서 (5개)
│   ├── 2026 전망/                 # 2026년 전망 보고서 (5개)
│   ├── Fixed Income/              # 채권 분석 (47개)
│   ├── SMR/                       # SMR 산업 분석 (13개)
│   ├── 글로벌 시장 브리핑/         # 글로벌 시장 브리핑 (73개)
│   ├── 글로벌 이슈/               # 글로벌 이슈 분석 (27개)
│   ├── 산업분석/                  # 산업 분석 (42개)
│   ├── 섹터_테마 리포트/           # 섹터/테마 리포트 (55개)
│   ├── 자산배분_전략/             # 자산배분 전략 (13개)
│   ├── 종목 리포트/               # 개별 종목 리포트 (50개)
│   ├── 주간_월간 정기물/           # 주간/월간 정기 간행물 (32개)
│   └── 투자전략/                  # 투자전략 보고서 (45개)
├── README.md                      # 프로젝트 개요 문서
├── OVERVIEW.md                    # ← 이 파일
└── .gitignore                     # 컴파일 파일, 캐시, 출력물 제외
```

---

## 3. 데이터 소스 매핑

| 시장 | 데이터 타입 | 소스 | API/라이브러리 |
|------|------------|------|--------------|
| **미국** | 거시 경제 지표 | FRED (Federal Reserve) | `FRED_API_KEY` |
| **미국** | 주가 데이터 | FMP (Financial Modeling Prep) | `FMP_API_KEY` |
| **미국** | 주가 폴백 | Yahoo Finance | `finance-datareader` |
| **미국** | FOMC 결정 | MANUAL | 수동 입력 |
| **한국** | 거시 경제 지표 | BOK ECOS (한국은행) | `BOK_API_KEY` |
| **한국** | 통계 데이터 | KOSIS (통계청) | `KOSIS_API_KEY` |
| **한국** | 주가 데이터 | Yahoo / FinanceDataReader | `finance-datareader` |

---

## 4. 미국 거시 지표 목록 (macro_sources.csv - 58개)

주요 이벤트 타입:
- **물가**: CPI (헤드라인/코어), PCE, PPI
- **고용**: NFP, 실업률, ADP
- **성장**: GDP, 소매판매, 산업생산
- **중앙은행**: FOMC (금리 결정)
- **주택**: 신규주택착공, 기존주택판매
- **서베이**: ISM 제조업/서비스업 PMI, 소비자신뢰지수

---

## 5. 한국 거시 지표 목록 (macro_sources.csv - 16개)

- CPI (소비자물가), GDP 성장률, 실업률
- 기준금리 (한국은행 정책금리)
- 무역수지, 경기선행지수 (CLI)
- PMI (제조업/서비스업)

---

## 6. 분석 파이프라인 흐름

```
[데이터 수집]
    fetch_macro_from_fred.py      → 미국 거시 데이터
    fetch_macro_from_bok_kosis.py → 한국 거시 데이터
    fetch_prices_fmp.py           → 주가 데이터 (캐싱 포함)
    seed_events_from_links.py     → 이벤트 레코드 생성
    backfill_events_from_fred.py  → 이벤트 날짜 백필
         ↓
[데이터 처리]
    merge_macro_actuals.py        → 거시 지표 + 실제값 병합
    compute_additional_windows.py → 수익률 윈도우 계산 (±5/10/21일)
         ↓
[통계 분석]
    compute_macro_impact.py       → OLS 회귀 (surprise_z → 섹터 수익률)
    compute_reaction_by_surprise_quantile.py → 서프라이즈 분위 분석
    compute_partial_impact.py     → 부분 영향 (순위 통제)
    join_cycles_into_reactions.py → 사이클 메트릭 결합
    build_reaction_matrix.py      → 반응 매트릭스 (wide format)
    analyze_focus_events.py       → 상위/하위 이벤트 분석
         ↓
[출력 생성]
    build_excel_dashboard_plus.py → Excel 워크북 (히트맵, 슬라이서)
    postprocess_add_overview_links.py → 개요 하이퍼링크 추가
         ↓
[릴리즈]
    GitHub Actions release.yml    → ZIP 패키징 & Release 발행
```

---

## 7. 출력 파일 목록 (`out/us/` 및 `out/kr/`)

| 파일명 | 설명 |
|--------|------|
| `reaction_heatmap_t0.csv` | 이벤트 당일(t0) 수익률 |
| `reaction_heatmap_win1.csv` | ±1일 윈도우 수익률 |
| `reaction_heatmap_win3.csv` | ±3일 윈도우 수익률 |
| `reaction_heatmap_win5.csv` | ±5일 윈도우 수익률 |
| `reaction_heatmap_win10.csv` | ±10일 윈도우 수익률 |
| `reaction_heatmap_win21.csv` | ±21일 윈도우 수익률 |
| `reaction_long.csv` | 롱폼 반응 데이터 |
| `macro_impact.csv` | 회귀 분석 결과 (beta, t-stat, n, asymmetry) |
| `reaction_by_surprise_quantile.csv` | 서프라이즈 크기별 분위 분석 |
| `partial_impact.csv` | 부분 상관 계수 |
| `focus_top_bottom.csv` | 상위/하위 성과 이벤트 |
| `focus_by_quantile.csv` | 분위별 분석 |
| `*_regime.csv` | 레짐 분리 분석 변형 |

**대시보드:**
- `dashboards/macro_dashboard_us.xlsx` - 미국 Excel 대시보드
- `dashboards/macro_dashboard_kr.xlsx` - 한국 Excel 대시보드

---

## 8. GitHub Actions 워크플로우

### nightly.yml (야간 파이프라인)
- **트리거**: 매일 17:00 UTC (02:00 KST)
- **런타임**: 미국 75분 + 한국 60분 (순차 실행)
- **Job 순서**: `build-us` → `build-kr` (의존성 체인)
- **단계별**:
  1. Checkout 저장소
  2. Python 3.11 설치
  3. 의존성 설치 (pandas, numpy, xlsxwriter, finance-datareader, openpyxl)
  4. 과거 가격/FRED 데이터 캐싱
  5. 전체 파이프라인 실행
  6. 아티팩트 업로드 (dashboards/*.xlsx, out/**/*.csv, logs)
- **아티팩트 보존**: 14일

### release.yml (릴리즈 패키징)
- **트리거**: 수동 실행 OR 야간 파이프라인 성공 후
- **작업**:
  1. 야간 아티팩트 다운로드
  2. ZIP 패키징 (dashboards-us.zip, dashboards-kr.zip, outputs-us.zip, outputs-kr.zip)
  3. 타임스탬프 태그로 GitHub Release 생성
  4. ZIP 파일 릴리즈 에셋으로 첨부

---

## 9. 경제 리포트 라이브러리 (`경제 리포트/` - 약 500개+ PDF)

자동화 파이프라인과 별개로 관리되는 **참조 자료 라이브러리**:

| 폴더 | 파일 수 | 내용 |
|------|--------|------|
| 2025 전망 | 5 | 채권/크레딧/주식/매크로/전략 전망 |
| 2026 전망 | 5 | 기술 트렌드, 글로벌 주식, 채권, 크레딧 |
| Fixed Income | 47 | 채권 주간/월간 코멘트 (2023-2025) |
| SMR | 13 | 소형모듈원전 산업 분석 |
| 글로벌 시장 브리핑 | 73 | 일일 글로벌 시장 스냅샷 (2022-2025) |
| 글로벌 이슈 | 27 | 어닝 리비전, 테마틱 레이더, 중국/ESG 분석 |
| 산업분석 | 42 | 반도체, 자동차, AI, 에너지, 인도, 배터리 |
| 섹터_테마 리포트 | 55 | 헬스케어, 보험, 운송, 기술, 우주, 인도 |
| 자산배분_전략 | 13 | 월간 자산배분, FX 전략, ETF 전략 |
| 종목 리포트 | 50 | NVDA, MSFT, GOOG, 삼성, SK하이닉스 등 |
| 주간_월간 정기물 | 32 | AI Weekly, ETF 리포트, 배터리 EV 뉴스 |
| 투자전략 | 45 | 주간 스냅샷, 어닝 리비전, 채권/크레딧 |

---

## 10. 로컬 실행 명령어

```bash
# 미국 파이프라인 전체 실행
cd C:\Users\ZIANNI\Documents\MACRO-DATA

# 1. 이벤트 시드
python scripts/seed_events_from_links.py --market us

# 2. FRED 데이터 사전 캐싱
python scripts/prefetch_fred_series.py

# 3. 이벤트 백필
python scripts/backfill_events_from_fred.py --market us

# 4. 가격 데이터 수집
python scripts/fetch_prices_fmp.py --market us

# 5. 거시 데이터 병합
python scripts/merge_macro_actuals.py --market us

# 6. 수익률 윈도우 계산
python scripts/compute_additional_windows.py --market us

# 7. 반응 매트릭스 구성
python scripts/build_reaction_matrix.py --market us

# 8. 매크로 임팩트 분석
python scripts/compute_macro_impact.py --market us

# 9. 분위 분석
python scripts/compute_reaction_by_surprise_quantile.py --market us

# 10. Excel 대시보드 생성
python scripts/build_excel_dashboard_plus.py --market us
python scripts/postprocess_add_overview_links.py --market us
```

---

## 11. 필수 환경 변수

```bash
FRED_API_KEY=<Federal Reserve FRED API 키>
FMP_API_KEY=<Financial Modeling Prep API 키>
BOK_API_KEY=<한국은행 ECOS API 키>
KOSIS_API_KEY=<통계청 KOSIS API 키>
```

---

## 12. 프로젝트 의도 요약

이 프로젝트는 **거시경제 이벤트가 주식/섹터 수익률에 미치는 영향을 정량화**하는 연구·투자 도구다.

**핵심 질문:**
- CPI 발표 서프라이즈(실제 vs 예측 차이)가 기술주/에너지주/금융주에 어떤 영향을 주는가?
- 매크로 이벤트의 영향이 경기 레짐(확장/둔화)에 따라 달라지는가?
- 서프라이즈 크기(분위)에 따라 시장 반응이 비선형적인가?

**사용 대상:**
- 매크로 리서처 및 포트폴리오 매니저
- 시스템 트레이더 (이벤트 드리븐 전략 개발)
- 투자 분석가 (섹터 로테이션 연구)

---

*이 문서는 Claude Code에 의해 자동 생성되었습니다. (2026-02-14)*
