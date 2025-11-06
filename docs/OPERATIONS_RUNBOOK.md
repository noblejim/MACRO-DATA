# 운영 런북 (Operations Runbook)

이 문서는 CI 스케줄(02:00 KST) 기반의 야간(nightly) 빌드와 Release 아티팩트 게시, 수동 재시작/복구, 데이터 품질 점검, 트러블슈팅을 한 곳에 정리합니다.

## 1. 스케줄 & 권장 설정
- 스케줄: 매일 02:00 KST (17:00 UTC) `nightly` 워크플로 자동 실행
- Release: 최근 성공한 `nightly`의 아티팩트를 모아 자동 게시
- 보존: Actions Retention 기간을 요구사항에 맞게 설정 (Settings → Actions → General)
- Secrets: 필수 키 4종 등록(FRED/FMP/BOK/KOSIS). 키 변경 시 즉시 재실행 권장

## 2. 수동 실행 절차
### 2.1 Nightly 수동 실행
1) Actions → `nightly` → “Run workflow” 클릭
2) Branch: `main` 선택 후 실행

### 2.2 Release 수동 실행
1) Actions → `release` → “Run workflow” 클릭
2) Branch: `main` 선택 후 실행
3) 주의: 직전에 성공한 `nightly`가 없으면 아티팩트 조회에서 404가 발생할 수 있음 → 먼저 `nightly` 한 번 성공시키기

## 3. 산출물(Artifacts) & Release
- nightly 아티팩트: `out/us/**`, `out/kr/**`, `dashboards/**/*.xlsx` 등
- release 자산(Assets): `dashboards-*.zip`, `outputs-*.zip`
- 다운로드: Releases 탭 또는 CLI (`gh release download -R noblejim/MACRO-DATA -p "dashboards-*.zip" -D downloads`)

## 4. 로컬 재현 / 점검
전제: Python 3.11, `pandas`, `numpy`, `requests`, `xlsxwriter` 설치 후 환경변수 설정(FRED/FMP/BOK/KOSIS)

### 4.1 US 파이프라인 최소경로
```
python scripts/fetch_prices_fmp.py --data-dir data/us --start 2000-01-01 --slice-years
python scripts/fetch_macro_from_fred.py --data-dir data/us
python scripts/merge_macro_actuals.py --data-dir data/us --backup
python scripts/compute_additional_windows.py --market us --data-dir data/us --out-dir out/us --windows 5,10,21
python scripts/compute_macro_impact.py --market us --out-dir out/us
python scripts/compute_reaction_by_surprise_quantile.py --market us --out-dir out/us
python scripts/compute_partial_impact.py --market us --out-dir out/us --by-regime
python scripts/analyze_focus_events.py --market us --out-dir out/us --last-days 180 --last-events 100 --regime-current
python scripts/join_cycles_into_reactions.py --market us --out-dir out/us --cycle-windows 21,63,126 --regimes-csv data/us/macro_regimes.csv
python scripts/build_excel_dashboard_plus.py --market us --data-dir data/us --out-dir out/us --last-days 365 --last-events 180 --dashboard-path dashboards/us_dashboard.xlsx
```

### 4.2 KR 파이프라인 최소경로
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

## 5. 데이터/스키마 품질 체크(권장)
- `data/us/macro_sources.csv` / `data/kr/macro_sources.csv`: 소스 ID/주기(M/Q)/매핑 정상 여부
- 중복/결측: 산출 CSV(`out/**`)에 중복 이벤트/결측치 있는지 확인
- 날짜 파싱: `pd.to_datetime` 포맷 경고 제거(ISO 형식 사용 권장)

## 6. 자주 발생하는 이슈 & 해결
- 404 (Release 아티팩트 다운로드 실패): 최근 성공 nightly 없음 → nightly 먼저 실행 성공
- `data/us/prices.csv` 없음: 가격 수집 미실행 → `fetch_prices_fmp.py` 먼저 수행
- Excel 시트명 오류(금지문자/길이): 시트명 정규화 로직 포함(최신 main 사용), 충돌 시 자동 `(2)`, `(3)` 접미사
- `writer.sheets['Reactions (+/-5)']` KeyError: 정규화된 이름 기반 조회로 수정됨
- FMP 레이트리밋/빈 결과: 백오프/재시도 + FDR 대체 경로 존재
- KR BOK/KOSIS 주기/파라미터 오류: `data/kr/macro_sources.csv` 주기(M/Q)와 KOSIS 파라미터(org/tbl/itm/objL*) 재검증 필요

## 7. 운영 권장사항
- Actions 보존기간과 로그 보관 주기 설정(법적/내부 정책 준수)
- 태그/릴리스 버전 관리(예: `v0.1.0`)로 사용자 안정판 제공
- README와 본 런북을 변경 시마다 PR로 갱신(운영 변경 추적)
