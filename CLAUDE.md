# MACRO-DATA Project

US/KR 거시경제 이벤트 자동 수집 → 섹터 영향 분석 → Excel 대시보드 생성 파이프라인.
API: FRED, FMP, BOK, KOSIS.

## Pipeline Flow
```
[수집] → [병합] → [반응행렬] → [통계분석] → [요약 생성] → [Excel 대시보드]
```

## Directory Structure
```
scripts/          # 21개 Python 스크립트 (파이프라인 전체)
data/us/          # US 소스: prices.csv, macro_events.csv, macro_sources.csv, tickers.csv, sector_benchmarks.csv, macro_regimes.csv
data/kr/          # KR 소스: 동일 구조 (BOK/KOSIS 기반)
out/us/, out/kr/  # 분석 결과: reaction_long.csv, macro_impact.csv, heatmap, cycle, focus, summary.json 등
dashboards/       # us_dashboard.xlsx, kr_dashboard.xlsx (25+ 시트)
docs/             # 문서, 인사이트 리포트
tests/            # pytest (3 테스트 파일)
reports/          # 미래에셋 경제 리포트 PDF (YYYY/MM/ 계층)
```

## Scripts (21개)

### 데이터 수집 (4개)
| Script | 역할 | Input | Output |
|--------|------|-------|--------|
| `fetch_prices_fmp.py` | US 주가 수집 (FMP API, FDR 폴백) | tickers.csv, sector_benchmarks.csv | data/us/prices.csv |
| `fetch_prices_fdr.py` | KR 주가 수집 (FinanceDataReader) | sector_benchmarks.csv | data/kr/prices.csv |
| `fetch_macro_from_fred.py` | US 매크로 58개 (FRED API, YoY/MoM 자동계산) | macro_events.csv, macro_sources.csv | macro_actuals.csv |
| `fetch_macro_from_bok_kosis.py` | KR 매크로 16개 (BOK/KOSIS API) | macro_sources.csv | macro_actuals.csv |

### 데이터 병합 (1개)
| Script | 역할 |
|--------|------|
| `merge_macro_actuals.py` | actuals → events CSV 병합 (백업 옵션) |

### 반응행렬 (3개)
| Script | 역할 |
|--------|------|
| `build_reaction_matrix.py` (672줄) | 이벤트별 섹터 반응 (t0, ±1~±21일), 사이클 모멘텀, 레짐별 평균 |
| `compute_additional_windows.py` | ±5/10/21일 추가 윈도우 (prefix cumprod 최적화) |
| `join_cycles_into_reactions.py` | 모멘텀/랭크 → reaction_long 병합 |

### 통계분석 (4개)
| Script | 역할 |
|--------|------|
| `compute_macro_impact.py` | OLS 회귀 (HC3 robust SE, BH FDR α=0.05) |
| `compute_partial_impact.py` | 부분 회귀 (모멘텀 통제 후 서프라이즈 효과) |
| `compute_reaction_by_surprise_quantile.py` | 서프라이즈 크기별 분위 분석 |
| `analyze_focus_events.py` | CPI/PCE/NFP/FOMC Top/Bottom 섹터 |

### 요약 생성 (1개)
| Script | 역할 |
|--------|------|
| `build_summary.py` | 파이프라인 결과 → out/{market}/summary.json 압축 |

### 대시보드 (2개)
| Script | 역할 |
|--------|------|
| `build_excel_dashboard_plus.py` | Excel 워크북 (히트맵, 조건부서식, freeze panes) |
| `postprocess_add_overview_links.py` | Overview 시트에 하이퍼링크 추가 |

### 유틸리티 (6개)
| Script | 역할 |
|--------|------|
| `config_defaults.py` | 상수 (CYCLE_WINDOWS=[21,63,126], FDR_ALPHA=0.05, MIN_SAMPLE=5) |
| `utils_date.py` | 날짜 파싱/포맷 (ISO 8601) |
| `utils_surprise.py` | surprise_z 계산 (빈도별 롤링윈도우, look-ahead 방지) |
| `utils_io.py` | CSV/Parquet 듀얼 I/O |
| `track_lineage.py` | JSONL 실행 이력 추적 |
| `seed_events_from_links.py` | KR 이벤트 캘린더 초기화 |

## Data Schema (핵심)

### macro_events.csv
`event_id, event_name, event_type, event_date, importance, expected_value, actual_value`

### macro_sources.csv
US: `event_type, source(FRED/FMP/MANUAL), series_id, freq(M/W/Q/D), compute_yoy, compute_mom, alt_series_ids`
KR: 위 + `stat_code, org_id, tbl_id, itm_id, objL1~8, param_mode, custom_url`

### prices.csv
`date, ticker, adj_close`

### reaction_long.csv (주요 출력)
`event_id, event_name, event_date, t0_date, sector, t0_return_avg, win1~21_cum_avg, surprise, surprise_z`

### macro_impact.csv (통계 결과)
`event_type, sector, metric, n, beta, t_stat, p_value, p_adj_bh, significant_bh, beta_pos/neg, t_pos/neg`

## Scale
- US: 58 매크로 이벤트, 23 종목, 11 섹터 벤치마크 (XLK~XLU), 5 레짐
- KR: 16 매크로 이벤트, KOSPI ETF 섹터 프록시, 11 섹터

## Known KR Data Issues (2026-02 감사 결과)
- CPI_YOY: 원시 인덱스(2,859~1,230,715), YoY% 아님
- CCSI: 누적합(45,366~45,914), 심리지수(60~120) 아님
- EMPLOYMENT = INFL_EXPECT: KOSIS 파라미터 오류로 동일 데이터 (r=1.000)
- POLICY_RATE: 2023년 2.00% 표시 (실제 3.50%, 1.5%p 차이)
- expected_value: 전체 542행 NULL (서프라이즈 계산 불가)
- GDP, TRADE_BAL, PMI, LEI, M2, USDKRW: 데이터 없음 (6/16 지표)
- 상세: docs/2026_KR_SECTOR_ANALYSIS.md Appendix A

## Quick Analysis (토큰 절약)
분석 요청 시 CSV 원본 대신 `out/{market}/summary.json`을 먼저 읽을 것.
- 핵심 통계 결과, 레짐별 Top/Bottom 섹터, 모멘텀 스냅샷, 데이터 품질 포함
- summary.json이 없거나 오래되었으면: `python scripts/build_summary.py`

## Local Run
```bash
cd C:\Users\ZIANNI\Documents\MACRO-DATA
# US
python scripts/fetch_prices_fmp.py
python scripts/fetch_macro_from_fred.py
python scripts/merge_macro_actuals.py --market us
python scripts/build_reaction_matrix.py --market us --compute-cycles
python scripts/compute_additional_windows.py --market us
python scripts/join_cycles_into_reactions.py --market us
python scripts/compute_macro_impact.py --market us
python scripts/build_summary.py --market us
python scripts/build_excel_dashboard_plus.py --market us

# KR: 동일 순서, --market kr + fetch_macro_from_bok_kosis.py 대신 사용
```

## User Preferences
- Git commit: 요청 시에만
- 개발/분석에 집중, commit/push 제안 불필요
