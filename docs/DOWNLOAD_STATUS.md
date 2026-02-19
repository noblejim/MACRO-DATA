# 미래에셋증권 리포트 다운로드 작업 현황

> 마지막 업데이트: 2026-02-16
> 재개 시 이 파일을 먼저 읽고 작업을 이어서 진행할 것

---

## 1. 작업 목표

- **대상 사이트**: https://securities.miraeasset.com/bbs/board/message/list.do?categoryId=1521
- **수집 범위**: 2025년 1월 1일 ~ 현재 (2026년 포함)
- **저장 경로**: `C:\Users\ZIANNI\Documents\MACRO-DATA\경제 리포트\`
- **목표 건수**: 2025년 약 2,553건 + 2026년 약 443건 = 약 2,996건

---

## 2. 현재 완료 현황

| 연도 | 다운 완료 | 목표 | 진행률 |
|------|----------|------|--------|
| 2025년 | **1,515개** | ~2,553개 | ~59% |
| 2026년 | **447개** | ~443개 | ~100% ✅ |
| **합계** | **1,962개** | ~2,996개 | ~65% |

### 2025년 월별 현황

| 월 | 완료 | 비고 |
|----|------|------|
| 01월 | 187개 | 부분 완료 (총 ~200건 추정) |
| 02월 | 27개 | ⚠️ 미흡 |
| 03월 | 18개 | ⚠️ 미흡 |
| 04월 | 22개 | ⚠️ 미흡 |
| 05월 | 19개 | ⚠️ 미흡 |
| 06월 | 9개 | ⚠️ 매우 미흡 |
| 07월 | 256개 | 양호 |
| 08월 | 163개 | 양호 |
| 09월 | 142개 | 양호 |
| 10월 | 242개 | 양호 |
| 11월 | 246개 | 양호 |
| 12월 | 184개 | 양호 |

**→ 2025년 2~6월 데이터가 현저히 부족. 재실행 시 해당 월들이 우선 수집됨.**

---

## 3. 사용한 스크립트

### `scripts/download_mirae_reports.py` (메인 다운로더)

```bash
# 기본 실행 (2025년 이후 전체)
cd C:\Users\ZIANNI\Documents\MACRO-DATA
python scripts/download_mirae_reports.py --start-year 2025 --delay 0.6

# 옵션
--start-year  수집 시작 연도 (기본: 2025)
--delay       PDF 다운로드 간격(초) (기본: 0.8, 권장: 0.6)
--save-dir    저장 경로 (기본: 경제 리포트 폴더)
```

**주요 동작:**
1. `collect_all_report_links()` → 연도×월 단위로 분할 수집 (사이트 82페이지 제한 우회)
2. 기존 파일은 자동 스킵 (attachmentId 기반 중복 제거)
3. `_index.csv` 자동 갱신
4. 완료 후 `다운로드: N | 스킵: N | 실패: N` 요약 출력

### `scripts/fix_mirae_filenames.py` (파일명 정리)

```bash
python scripts/fix_mirae_filenames.py
```

**동작:** 깨진 한글 파일명(EUC-KR 인코딩 오류)을 attachmentId로 매핑하여 올바른 한글 파일명으로 리네임

---

## 4. 핵심 기술 이슈 및 해결책

### 4-1. 사이트 페이지 한도 (핵심)
- **문제**: 연간 검색 시 최대 82페이지(약 820건)만 반환
- **해결**: `collect_all_report_links()`를 **월별 분할** 수집으로 변경
  - `collect_year_reports(yr, yr, month_start=M, month_end=M, day_end=D)`
  - 한 달에 최대 ~200건이므로 82페이지 제한 내에서 처리 가능

### 4-2. Rate Limiting
- **문제**: 연속 요청 시 사이트가 부분 HTML 반환 (4~5건/페이지, next 링크 없음)
- **해결**:
  - 페이지 간 0.4초 대기
  - 월 전환 시 1.0초 대기
  - 첫 요청 전 1.5초 대기
  - 스크립트 재시작 전 5~10초 대기

### 4-3. 커서 기반 페이지네이션
- **문제**: `startId=zzzzz~`를 모든 페이지에 사용하면 항상 1페이지 반환
- **해결**: `find_page_link(html, page+1)` — HTML 내 다음 페이지 링크에서 `startId` 커서를 추출
  - 10페이지 블록 단위로 커서 변경됨 (예: `startId=09nq3~`)

### 4-4. 한글 파일명 깨짐
- **문제**: 사이트 HTML이 EUC-KR 인코딩, strict 디코딩 시 깨짐
- **해결**: `raw.decode('euc-kr', errors='ignore')`

### 4-5. PDF URL 파싱
- **패턴**: `downConfirm('https://...pdf?attachmentId=NNNNNNN')`
- **정리**: `re.sub(r'(attachmentId=\d+).*', r'\1', pdf_url)` — 잡음 제거

---

## 5. 사용한 도구 / MCP

| 도구 | 용도 |
|------|------|
| **Claude Code (Bash)** | Python 스크립트 실행, 백그라운드 프로세스 관리 |
| **Read / Edit / Write** | 스크립트 파일 읽기/수정/생성 |
| **Grep / Glob** | 코드 패턴 검색 |
| **Task (background agent)** | 장시간 다운로드를 백그라운드에서 실행 |
| **MCP 없음** | 사이트 접근은 순수 Python `urllib` 사용 (Selenium/Playwright 불필요) |

---

## 6. 이어서 작업하는 방법

### 단계 1: 추가 다운로드 실행

```bash
cd C:\Users\ZIANNI\Documents\MACRO-DATA
python scripts/download_mirae_reports.py --start-year 2025 --delay 0.6
```

- 기존 파일은 자동 스킵되므로 반복 실행해도 안전
- 한 번에 약 300~600건 추가 수집됨 (rate limit으로 월당 부분 수집)
- **2~3회 반복 실행**하면 2025년 전체 수집 가능

### 단계 2: 파일명 정리

```bash
python scripts/fix_mirae_filenames.py
```

### 단계 3: 완료 확인

```python
import os
from collections import Counter
folder = r'C:\Users\ZIANNI\Documents\MACRO-DATA\경제 리포트'
pdfs = [f for f in os.listdir(folder) if f.endswith('.pdf')]
years = Counter(f[:4] for f in pdfs)
months = Counter(f[5:7] for f in pdfs if f.startswith('2025'))
print(f'총 {len(pdfs)}개')
print('연도별:', dict(sorted(years.items())))
print('2025 월별:', dict(sorted(months.items())))
```

---

## 7. 파일 구조

```
경제 리포트/
├── DOWNLOAD_STATUS.md        ← 이 파일
├── _index.csv                ← 전체 리포트 인덱스 (date, title, filename, pdf_url)
├── 2025-01-02_삼성전자_...pdf
├── 2025-01-02_...pdf
└── ...
```

---

## 8. 참고: 사이트 URL 구조

```
# 월별 검색 URL 예시 (2025년 3월)
https://securities.miraeasset.com/bbs/board/message/list.do
  ?categoryId=1521
  &searchType=2
  &searchStartYear=2025&searchStartMonth=03&searchStartDay=01
  &searchEndYear=2025&searchEndMonth=03&searchEndDay=31
  &listType=1&startId=zzzzz~&startPage=1&curPage=1&direction=1

# 다음 페이지 (커서 방식)
# curPage=11 부터 startId가 변경됨 (예: startId=09nq3~)
# find_page_link(html, page+1) 로 HTML에서 직접 추출
```
