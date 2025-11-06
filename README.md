# MACRO-DATA

[![nightly](https://github.com/noblejim/MACRO-DATA/actions/workflows/nightly.yml/badge.svg?branch=main)](https://github.com/noblejim/MACRO-DATA/actions/workflows/nightly.yml)
[![release](https://github.com/noblejim/MACRO-DATA/actions/workflows/release.yml/badge.svg?branch=main)](https://github.com/noblejim/MACRO-DATA/actions/workflows/release.yml)
[![Latest Release](https://img.shields.io/github/v/release/noblejim/MACRO-DATA?display_name=tag)](https://github.com/noblejim/MACRO-DATA/releases)

미국/한국 거시경제 데이터 파이프라인과 대시보드 자동화 저장소입니다. 매일 정해진 시간(02:00 KST)에 원천 데이터 수집 → 반응/임팩트 분석 → Excel 대시보드 산출까지 자동으로 수행하고, 성공 시 Release 아티팩트로 게시합니다.

## 특징
- US/KR 파이프라인: FRED/FMP(US), BOK/KOSIS(KR) 데이터 수집, 캐시/재시도/슬라이싱 지원
- 반응 분석: 이벤트 기준 ±5/±10/±21 윈도우, Surprise quantile, Partial impact, Regime별 분석
- 대시보드: Controls/Parameters 포함, Focus Top/Bottom/Quantile, Heatmap/Matrix 시트 제공
- CI 자동화: nightly(스케줄) → release(아티팩트 패키징) 워크플로

## 바로 사용하기 (Release 다운로드)
1) GitHub 상단의 “Releases” 탭 이동
2) 최신 릴리스에서 자산(Assets)을 다운로드
   - `dashboards-us.zip` / `dashboards-kr.zip` (예: `dashboards/us_dashboard.xlsx` 포함)
   - `outputs-us.zip` / `outputs-kr.zip` (중간 산출물 CSV 전체)

CLI로 받기(선택):
```
# 예: 모든 대시보드 패키지 다운로드
gh release download -R noblejim/MACRO-DATA -p "dashboards-*.zip" -D downloads
```

## Secrets (필수)
Repository → Settings → Secrets and variables → Actions에 아래 Key를 추가하세요.
- `FRED_API_KEY` (US)
- `FMP_API_KEY` (US)
- `BOK_API_KEY` (KR, 선택이지만 권장)
- `KOSIS_API_KEY` (KR, 선택이지만 권장)

## 워크플로
- Nightly: 매일 02:00 KST(= 17:00 UTC) 실행, 성공 시 아티팩트 생성
- Release: 최근 성공한 nightly 아티팩트를 패키징하여 Release에 게시

수동 실행 방법:
- Nightly 수동 실행: Actions → nightly → “Run workflow” (Branch: `main`)
- Release 수동 실행: Actions → release → “Run workflow” (Branch: `main`)
  - 주의: 최근 성공한 nightly 런이 없다면 Release 다운로드 단계가 404가 날 수 있습니다. 먼저 nightly를 1회 성공시키세요.

## 로컬 실행 (선택)
전제: Python 3.11, `pandas`, `numpy`, `requests`, `xlsxwriter` 설치. 환경변수 설정:
```
# PowerShell 예시
$env:FMP_API_KEY="<YOUR_FMP_KEY>"
$env:FRED_API_KEY="<YOUR_FRED_KEY>"
$env:BOK_API_KEY="<YOUR_BOK_KEY>"      # KR 사용 시 권장
$env:KOSIS_API_KEY="<YOUR_KOSIS_KEY>"  # KR 사용 시 권장
```

US 파이프라인 (요약):
```
python scripts/fetch_prices_fmp.py --data-dir data/us --start 2000-01-01 --slice-years
python scripts/fetch_macro_from_fred.py --data-dir data/us
python scripts/merge_macro_actuals.py --data-dir data/us --backup
python scripts/compute_additional_windows.py --market us --data-dir data/us --out-dir out/us --windows 5,10,21
python scripts/compute_macro_impact.py --market us --out-dir out/us
python scripts/compute_reaction_by_surprise_quantile.py --market us --out-dir out/us
python scripts/compute_partial_impact.py --market us --out-dir out/us --by-regime
python scripts/analyze_focus_events.py --market us --out-dir out/us --last-days 180 --last-events 100 --regime-current
# 사이클 메트릭 결합
python scripts/join_cycles_into_reactions.py --market us --out-dir out/us --cycle-windows 21,63,126 --regimes-csv data/us/macro_regimes.csv
# 대시보드
python scripts/build_excel_dashboard_plus.py --market us --data-dir data/us --out-dir out/us --last-days 365 --last-events 180 --dashboard-path dashboards/us_dashboard.xlsx
```

KR 파이프라인 (요약):
```
python scripts/fetch_macro_kr_bok.py   --data-dir data/kr
python scripts/fetch_macro_kr_kosis.py --data-dir data/kr
python scripts/merge_macro_actuals.py  --data-dir data/kr --backup
python scripts/compute_additional_windows.py --market kr --data-dir data/kr --out-dir out/kr --windows 5,10,21
python scripts/compute_macro_impact.py --market kr --out-dir out/kr
python scripts/compute_reaction_by_surprise_quantile.py --market kr --out-dir out/kr
python scripts/compute_partial_impact.py --market kr --out-dir out/kr --by-regime
python scripts/analyze_focus_events.py --market kr --out-dir out/kr --last-days 180 --last-events 100 --regime-current
python scripts/build_excel_dashboard_plus.py --market kr --data-dir data/kr --out-dir out/kr --last-days 365 --last-events 180 --dashboard-path dashboards/kr_dashboard.xlsx
```

## 디렉터리 개요
- `scripts/` 파이프라인 스크립트
- `data/us`, `data/kr` 원천/머지 데이터, 매핑/소스 정의
- `out/us`, `out/kr` 중간 산출물 CSV
- `dashboards/` 결과 Excel 파일
- `.github/workflows/` CI 스케줄 및 릴리스 설정

## 트러블슈팅
- Release 다운로드 404: 최근 성공한 nightly 런이 없을 때 발생 → nightly를 먼저 실행/성공
- `data/us/prices.csv` 누락: 가격 수집 미실행 → `fetch_prices_fmp.py` 먼저 실행
- Excel 시트명 오류(금지문자/길이): 코드에서 시트명 자동 정규화 적용됨 (문제 발생 시 최신 main 반영)
- `writer.sheets['Reactions (+/-5)']` KeyError: 정규화된 시트명 사용으로 해결됨 (자동 매핑 적용)
- FMP 레이트리밋/빈 결과: 재시도/백오프, FDR 대체 경로 적용 (환경에 따라 차이 가능)
- KR BOK/KOSIS 주기/파라미터: `data/kr/macro_sources.csv` 주기(M/Q)와 KOSIS 파라미터 확인 필요

## 기여
PR 환영합니다. 문서/런북/소스 정밀화/매핑 보강 등 제안해주세요.
