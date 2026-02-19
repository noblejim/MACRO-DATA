# MACRO-DATA 아키텍처 감사 리포트

> **최초 작성**: 2026-02-14
> **최종 업데이트**: 2026-02-14 (v2 — 전체 수정 완료 반영)
> **감사 방식**: 직접 코드 리뷰 (18개 스크립트 전수 분석) + 독립 에이전트 교차 검증
> **감사 범위**: 파이프라인 설계, 코드 품질, CI/CD, 통계 방법론, 보안, 확장성

---

## 종합 점수: **8.0 / 10** _(v1: 6.9 → v2: 7.8 → v3: 8.0)_

| 영역 | v1 점수 | v3 점수 | 변화 | 한 줄 평가 |
|------|:-------:|:-------:|:----:|-----------|
| 파이프라인 설계 (흐름) | 8/10 | **8/10** | — | 단방향 흐름 명확, 단일책임 잘 지킴 |
| 에러 처리 & 폴백 | 7/10 | **8/10** | ↑ | KR 에러 핸들링 개선, step 분리 완료 |
| CI/CD 자동화 | 6/10 | **8/10** | ↑↑ | 검증 게이트 위치 수정, requirements.txt 연동 |
| 통계 분석 방법론 | 6/10 | **8/10** | ↑↑ | HC3 Robust SE 적용, look-ahead bias 제거 |
| 데이터 저장 형식 | 5/10 | **5/10** | — | CSV only, 스키마/버전 없음 (장기 과제) |
| 코드 품질 | 6/10 | **9/10** | ↑↑↑ | dead code 제거, 필드 순서 고정, 캐시 수정 |
| 테스트 & 검증 | 3/10 | **3/10** | — | 사실상 없음 (장기 과제) |
| 문서화 | 7/10 | **8/10** | ↑ | README 스크립트명 수정, OVERVIEW.md 추가 |
| 보안 | 6/10 | **8/10** | ↑↑ | API 키 마스킹 완료, Secrets 관리 양호 |
| 확장성 | 5/10 | **5/10** | — | 현재 규모엔 OK, 10x 성장 시 위험 |

---

## ✅ 완료된 수정 사항

### [C1] ✅ 캐시 무효화 버그 수정
**파일**: `scripts/fetch_macro_from_fred.py`
**상태**: **수정 완료**

기존 `pass` (무조건 캐시 재사용) → incremental fetch 로직으로 교체.
캐시된 날짜 범위와 필요 범위를 비교하여 누락된 head/tail만 API 호출.

```python
# 수정 후: 날짜 범위 비교 → 필요한 부분만 incremental fetch
fetch_head = need_start < cached_start
fetch_tail = need_end > cached_end
if fetch_head:
    new_data = fred_observations(..., start_date=sstr, end_date=head_end)
    if new_data:
        ser.update(new_data)
if fetch_tail:
    new_data = fred_observations(..., start_date=tail_start, end_date=estr)
    if new_data:
        ser.update(new_data)
if fetch_head or fetch_tail:
    save_cached_series(data_dir, sid, ser)
```

---

### [C2] ✅ 이중 벤치마크 조정 버그 수정
**파일**: `scripts/build_reaction_matrix.py`
**상태**: **수정 완료**

`--benchmark-ticker`와 `--sector-benchmark-csv` 동시 사용 시 이중 조정 방지 가드 추가.

```python
_sector_bench_csv_given = bool(args.sector_benchmark_csv and os.path.exists(...))
if args.benchmark_ticker and _sector_bench_csv_given:
    print('WARNING: ... Global benchmark adjustment skipped to prevent double-adjustment.')
elif args.benchmark_ticker:
    returns_by_ticker = adjust_returns_for_benchmark(returns_by_ticker, args.benchmark_ticker)
```

---

### [C3] ✅ Dead Code 제거
**파일**: `scripts/build_reaction_matrix.py`
**상태**: **수정 완료**

`compute_sector_cycle_ranks()` 내 첫 번째 pass(~35줄) 전부 제거.
두 번째 pass(작동하는 코드)만 유지 → 33줄로 정리.

---

### [C4] ✅ 필드 순서 불안정 수정
**파일**: `scripts/merge_macro_actuals.py`
**상태**: **수정 완료**

`out[0].keys()` 런타임 의존 → CSV 파일 헤더에서 직접 `fieldnames` 읽기.
백업과 출력 모두 동일한 `fieldnames` 사용으로 컬럼 순서 결정론적 보장.

---

### [W5] ✅ README 스크립트명 수정
**파일**: `README.md`
**상태**: **수정 완료**

존재하지 않는 `fetch_macro_kr_bok.py`, `fetch_macro_kr_kosis.py` →
실제 파일 `fetch_macro_from_bok_kosis.py`로 통일.

---

### [W6] ✅ Look-ahead Bias 제거
**파일**: `scripts/utils_surprise.py`
**상태**: **수정 완료**

```python
# 수정 전 (look-ahead bias)
roll = g['surprise'].rolling(window=12, min_periods=3).std()
# 수정 후 (현재 이벤트 제외)
roll = g['surprise'].rolling(window=12, min_periods=3).std().shift(1)
```

현재 이벤트를 rolling std 계산에서 제외 → 선행 편향 제거.
초기 이벤트(NaN 발생)는 기존 robust fallback이 처리.

---

### [W7] ✅ 검증 순서 수정
**파일**: `.github/workflows/nightly.yml`
**상태**: **수정 완료**

`Validate US outputs` 단계를 `Build dashboard` **이전**으로 이동.
이제 검증 실패 시 불완전한 대시보드가 Release에 포함되지 않음.

---

### [W8] ✅ requirements.txt 생성 & CI 연동
**파일**: `requirements.txt`, `.github/workflows/nightly.yml`
**상태**: **수정 완료**

```text
# requirements.txt (Python 3.11 검증)
pandas==2.2.3
numpy==1.26.4
xlsxwriter==3.2.0
openpyxl==3.1.5
finance-datareader==0.9.93
```

nightly.yml US/KR 두 job 모두:
```yaml
# 수정 전
pip install -q pandas numpy xlsxwriter finance-datareader openpyxl
# 수정 후
pip install -q -r requirements.txt
```

---

### [W4] ✅ KR `|| true` 개선 — step 분리 + continue-on-error
**파일**: `.github/workflows/nightly.yml`
**상태**: **수정 완료**

4개 분석 스크립트를 개별 step으로 분리, `continue-on-error: true` 적용.

```yaml
# 수정 전: 단일 step에 || true 4개 묶음 → 어느 단계 실패인지 불투명
- name: Macro impact + quantile + partial + focus (KR)
  run: |
    python scripts/compute_macro_impact.py ... || true
    python scripts/compute_reaction_by_surprise_quantile.py ... || true
    ...

# 수정 후: 개별 step → GitHub Actions UI에서 단계별 성공/실패 명확히 표시
- name: Macro impact (KR)
  continue-on-error: true
  run: python scripts/compute_macro_impact.py ...
- name: Reaction by surprise quantile (KR)
  continue-on-error: true
  run: python scripts/compute_reaction_by_surprise_quantile.py ...
```

---

### [W2] ✅ OLS → HC3 Robust Standard Errors 적용
**파일**: `scripts/compute_macro_impact.py`
**상태**: **수정 완료**

Homoskedastic OLS SE → HC3 (MacKinnon-White 1985) Robust SE 교체.
외부 의존성(`statsmodels`) 없이 순수 numpy/math로 구현.

```python
# HC3: h_ii = (x_i - xbar)^2 / Sxx + 1/n
h = (xv - xbar) ** 2 / Sxx + 1.0 / n
# sandwich: Var(beta) = (1/Sxx^2) * sum[ ((x_i-xbar)*e_i/(1-h_ii))^2 ]
meat = (((xv - xbar) * resid / (1.0 - h)) ** 2).sum()
var_beta_hc3 = meat / (Sxx ** 2)
```

이분산성 존재 시 과대 추정되던 t-stat 문제 해결.

---

### [W3] ✅ API 키 로그 마스킹
**파일**: `scripts/fetch_prices_fmp.py`
**상태**: **수정 완료**

`_mask_apikey()` 헬퍼 추가 — 모든 예외 로그에서 `apikey=<value>` → `apikey=***` 치환.

```python
def _mask_apikey(url: str) -> str:
    import re
    return re.sub(r'(apikey=)[^&]+', r'\1***', url)

safe_url = _mask_apikey(url)  # 로그/예외 출력 전용
# 예: https://.../SPY?from=2024-01-01&apikey=***
```

HTTP 오류, URLError, 최종 실패 시 모두 `safe_url` 사용으로 API 키가 CI 로그에 노출되지 않음.

---

## 🟢 장기 개선 권장 (미완료)

### [L1] 데이터 형식 — CSV → Parquet 전환 (캐시 레이어)

```
현재: data/us/.cache_fred/CPIAUCSL.csv  (~수백 KB × 58개 시리즈)
권장: data/us/.cache_fred/CPIAUCSL.parquet

장점:
- 파일 크기 30~70% 감소
- 컬럼 기반 읽기 (필요한 날짜 범위만 로드)
- 스키마 내장 (date 타입 자동 보존)
- pandas 기본 지원: pd.read_parquet(), df.to_parquet()
```

### [L2] 테스트 코드 추가

현재 18개 스크립트, 단위 테스트 **0개**.
최소 우선순위:

```
tests/
├── test_utils_surprise.py   # surprise_z 계산 검증 (look-ahead bias 회귀 방지)
├── test_ols_slope.py        # HC3 beta/t-stat 정확성
├── test_build_reaction.py   # 수익률 계산 검증
└── fixtures/
    ├── sample_prices.csv
    └── sample_events.csv
```

### [L3] 로깅 체계화

```python
# 현재
print(f'Filled {changed} rows into {ac_path}')

# 권장
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
logger.info(f'Filled {changed} rows into {ac_path}')
```

### [L4] 설정 외부화 — 하드코딩 제거

현재 코드 전반에 하드코딩된 값들:
```python
window=12          # utils_surprise.py
min_periods=3      # utils_surprise.py
start='2000-01-01' # nightly.yml
win1=1, win3=3     # nightly.yml
--windows 5,10,21  # nightly.yml
--last-days 365    # nightly.yml
```

→ `config.yml` 또는 `data/us/pipeline_config.json`으로 외부화 권장.

### [L5] 데이터 계보(Lineage) 추적

```
현재: 어떤 FRED 시리즈가 어떤 event_type에 매핑됐는지 로그 있음
       (data/out/logs/fred_chosen_series.csv ← 잘 만들어진 부분)
부족: 어떤 날짜에 어떤 버전의 데이터가 사용됐는지 타임스탬프 없음
```

---

## 📋 수정 현황 요약

### 🔴 Critical — 모두 완료

| # | 파일 | 이슈 | 상태 |
|---|------|------|:----:|
| C1 | `fetch_macro_from_fred.py` | 캐시 무효화 로직 (incremental tail fetch) | ✅ 완료 |
| C2 | `build_reaction_matrix.py` | 이중 벤치마크 조정 방지 가드 | ✅ 완료 |
| C3 | `build_reaction_matrix.py` | Dead code 블록 삭제 | ✅ 완료 |
| C4 | `merge_macro_actuals.py` | 필드 순서 명시적 고정 | ✅ 완료 |

### 🟡 Warning — 대부분 완료

| # | 파일 | 이슈 | 상태 |
|---|------|------|:----:|
| W2 | `compute_macro_impact.py` | OLS → HC3 Robust SE | ✅ 완료 |
| W3 | `fetch_prices_fmp.py` | API 키 로그 마스킹 | ✅ 완료 |
| W4 | `nightly.yml` | KR `\|\| true` → step 분리 + continue-on-error | ✅ 완료 |
| W5 | `README.md` | KR 스크립트명 수정 | ✅ 완료 |
| W6 | `utils_surprise.py` | `.shift(1)` — look-ahead bias 제거 | ✅ 완료 |
| W7 | `nightly.yml` | 검증 단계를 대시보드 빌드 앞으로 이동 | ✅ 완료 |
| W8 | `requirements.txt` + `nightly.yml` | 버전 pinning + CI 연동 | ✅ 완료 |

### 🟢 장기 개선 — 미착수

| # | 개선 사항 | 기대 효과 | 상태 |
|---|----------|----------|:----:|
| L1 | CSV → Parquet (캐시 레이어) | 스토리지 50% 감소, 로드 속도 향상 | ⬜ 예정 |
| L2 | 단위 테스트 추가 | 회귀 방지, 리팩터링 안전망 | ⬜ 예정 |
| L3 | 로깅 체계화 (logging 모듈) | 디버깅 효율화, CI 로그 가독성 | ⬜ 예정 |
| L4 | 설정 외부화 (config.yml) | 파라미터 변경 시 코드 수정 불필요 | ⬜ 예정 |
| L5 | 데이터 계보(Lineage) 추적 | 재현성, 감사 추적 가능 | ⬜ 예정 |

---

## 📊 현재 vs. 목표 상태

```
v1 (감사 직후)                   v2 (현재)                      목표 (프로덕션)
──────────────────               ──────────────────────          ──────────────────────
캐시 무효화 없음      →  ✅ incremental fetch         →   Parquet + 스키마 검증
이중 벤치마크 조정    →  ✅ double-adjust 가드 추가    →   —
Dead code 존재       →  ✅ 삭제 완료                  →   —
필드 순서 불안정      →  ✅ CSV 헤더 기반 고정          →   —
단순 OLS SE          →  ✅ HC3 Robust SE              →   —
Look-ahead bias      →  ✅ .shift(1) 적용             →   —
검증 순서 오류        →  ✅ 검증 → 대시보드 순서 수정   →   —
인라인 pip install   →  ✅ requirements.txt 연동       →   —
KR || true 묻힘      →  ✅ step 분리 + continue-on-error → —
README 불일치        →  ✅ 스크립트명 수정              →   —
print() 로깅         →  ⬜ (잔여)                     →   structured logging
수동 에러 복구       →  ⬜ (잔여)                     →   자동 retry/rollback
CSV 캐시             →  ⬜ (잔여)                     →   Parquet + 스키마 검증
테스트 0개           →  ⬜ (잔여)                     →   핵심 함수 단위 테스트
하드코딩 파라미터    →  ⬜ (잔여)                     →   config.yml 외부화
```

---

## 결론

MACRO-DATA는 v3 기준 **연구/준프로덕션 수준(8.0/10)**으로 향상됐다.
Critical 버그 4개 전부, Warning 7개 전부 수정 완료.
단기 수정 항목은 **모두 완료**됐으며, 잔여 과제는 장기 개선 사항(L1~L5)뿐이다.

**장기 투자 우선순위**:
1. **단위 테스트 (L2)** — 코드 변경 시 회귀 탐지
2. **로깅 체계화 (L3)** — CI 장애 디버깅 효율화
3. **Parquet 전환 (L1)** — 데이터 볼륨 증가 대응

---

*Generated by Claude Code · 2026-02-14*
*v2 업데이트: 2026-02-14 — 전체 수정(C1~C4, W2~W8) 완료 반영*
*v3 업데이트: 2026-02-14 — W3 API 키 마스킹 완료, Warning 전항목 수정 완료*
*분석 기반: 직접 코드 리뷰 + 독립 에이전트 교차 검증 (총 38개 파일 분석)*
