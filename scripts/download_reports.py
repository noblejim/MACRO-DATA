# Mirae Asset Securities research report downloader
# Downloads all reports published since start_year (default: 2025)
# Skips already-downloaded files
import os
import re
import time
import logging
import argparse
import gzip
import zlib
from urllib.request import urlopen, Request
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.error import HTTPError, URLError
from io import BytesIO

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

BASE_URL = 'https://securities.miraeasset.com'
LIST_URL = f'{BASE_URL}/bbs/board/message/list.do'
CATEGORY_ID = '1521'
# 저장 경로 (경제 리포트)
SAVE_DIR = os.path.join(os.path.expanduser('~'), 'Documents', 'MACRO-DATA',
                        '\uacbd\uc81c \ub9ac\ud3ec\ud2b8')


def _headers():
    return {
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36'),
        'Referer': f'{LIST_URL}?categoryId={CATEGORY_ID}',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }


def decode_response(resp):
    raw = resp.read()
    # 압축 해제
    encoding = resp.headers.get('Content-Encoding', '')
    if encoding == 'gzip':
        try:
            raw = gzip.decompress(raw)
        except Exception:
            pass
    elif encoding == 'deflate':
        try:
            raw = zlib.decompress(raw)
        except Exception:
            try:
                raw = zlib.decompress(raw, -zlib.MAX_WBITS)
            except Exception:
                pass
    # 인코딩 감지 (EUC-KR 우선, 오류는 무시)
    for enc in ('euc-kr', 'cp949', 'utf-8'):
        try:
            return raw.decode(enc, errors='ignore')
        except Exception:
            continue
    return raw.decode('utf-8', errors='ignore')


def fetch_html(url, timeout=25, max_attempts=4):
    for attempt in range(max_attempts):
        try:
            req = Request(url, headers=_headers())
            with urlopen(req, timeout=timeout) as resp:
                return decode_response(resp)
        except HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(min(30, 2 ** attempt))
                continue
            logger.error(f'HTTP {e.code}: {url}')
            return ''
        except (URLError, Exception) as e:
            time.sleep(min(30, 2 ** attempt))
            continue
    logger.error(f'Failed after {max_attempts} attempts: {url}')
    return ''


def parse_reports(html):
    """[(date, title, pdf_url), ...]"""
    reports = []
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
    for row in rows:
        date_m = re.search(r'(202\d-\d{2}-\d{2})', row)
        if not date_m:
            continue
        date = date_m.group(1)

        title_m = re.search(r'id="bbsTitle\d+"[^>]*>(.*?)</a>', row, re.DOTALL | re.IGNORECASE)
        if not title_m:
            continue
        title = re.sub(r'<[^>]+>', '', title_m.group(1))
        title = re.sub(r'\s+', ' ', title.replace('\n', ' ')).strip()

        pdf_m = re.search(r"downConfirm\('(https?://[^']+)'", row)
        if not pdf_m:
            continue
        pdf_url = pdf_m.group(1).strip()
        if not re.search(r'\.pdf', pdf_url, re.IGNORECASE):
            continue
        # 잘린 URL 정리: attachmentId 숫자 이후 잡음 제거
        pdf_url = re.sub(r'(attachmentId=\d+).*', r'\1', pdf_url)

        reports.append((date, title, pdf_url))
    return reports


def extract_next_page_url(html):
    """다음 페이지 링크 추출 (페이지 번호 순서대로 찾기)."""
    # 페이지 링크: list.do?categoryId=...&curPage=N&direction=1
    page_links = re.findall(
        r'href="(list\.do\?[^"]+curPage=(\d+)[^"]*)"',
        html, re.IGNORECASE
    )
    if not page_links:
        return None
    # 현재 페이지 (class="on") 찾기
    cur_m = re.search(r'class="on"[^>]*>(\d+)<', html)
    cur_page = int(cur_m.group(1)) if cur_m else 1

    def build_url(href):
        href = href.replace('&amp;', '&')
        if href.startswith('http'):
            return href
        # 상대경로: /bbs/board/message/list.do?...
        if href.startswith('/'):
            return BASE_URL + href
        return BASE_URL + '/bbs/board/message/' + href

    # cur_page + 1 링크 찾기
    for href, pg_num in page_links:
        if int(pg_num) == cur_page + 1:
            return build_url(href)
    # 없으면 가장 큰 번호
    candidates = [(int(pg), href) for href, pg in page_links]
    candidates.sort()
    if candidates:
        max_pg, href = candidates[-1]
        if max_pg > cur_page:
            return build_url(href)
    return None


def get_total_count(html):
    for pat in [r'전체건수\s*[：:]\s*<span>(\d+)</span>', r'<span>(\d+)</span>\s*건']:
        m = re.search(pat, html)
        if m:
            return int(m.group(1))
    return 0


def safe_filename(date, title, pdf_url):
    att_m = re.search(r'attachmentId=(\d+)', pdf_url)
    if not att_m:
        att_m = re.search(r'/(\d+)\.pdf', pdf_url)
    att_id = att_m.group(1) if att_m else '0'
    # 한글 파일명 안전 처리: ASCII + 한글(유니코드) 모두 허용, 특수문자만 치환
    safe = re.sub(r'[\\/:*?"<>|]', '_', title)
    safe = re.sub(r'\s+', ' ', safe).strip()[:70]
    # 파일명 인코딩 테스트 (Windows 경로 안전성)
    try:
        safe.encode('utf-8')
    except Exception:
        safe = att_id  # fallback
    return f'{date}_{safe}_{att_id}.pdf'


def download_pdf(pdf_url, save_path, max_attempts=4):
    for attempt in range(max_attempts):
        try:
            req = Request(pdf_url, headers=_headers())
            with urlopen(req, timeout=45) as resp:
                raw = resp.read()
                enc = resp.headers.get('Content-Encoding', '')
                if enc == 'gzip':
                    try:
                        raw = gzip.decompress(raw)
                    except Exception:
                        pass
            if len(raw) < 500:
                return False
            with open(save_path, 'wb') as f:
                f.write(raw)
            return True
        except HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(min(20, 2 ** attempt))
                continue
            return False
        except Exception:
            time.sleep(min(20, 2 ** attempt))
            continue
    return False


def find_page_link(html, target_page):
    """HTML에서 특정 페이지 번호의 링크 추출 (startId 커서 포함)."""
    page_links = re.findall(
        r'href="((?:/bbs/board/message/)?list\.do\?[^"]+curPage=(\d+)[^"]*)"',
        html, re.IGNORECASE
    )
    for href, pg in page_links:
        if int(pg) == target_page:
            href = href.replace('&amp;', '&')
            if href.startswith('http'):
                return href
            if href.startswith('/'):
                return BASE_URL + href
            return BASE_URL + '/bbs/board/message/' + href
    return None


def collect_year_reports(year_start, year_end,
                         month_start=None, month_end=None, day_end=None):
    """특정 연도/월 범위의 리포트 링크 수집."""
    all_reports = []
    seen_ids = set()

    # 1페이지: startId=zzzzz~ 고정 시작
    sm = f'{month_start:02d}' if month_start else '01'
    em = f'{month_end:02d}' if month_end else '12'
    sd = '01'
    ed = f'{day_end:02d}' if day_end else '31'
    first_url = (
        f'{LIST_URL}?categoryId={CATEGORY_ID}'
        f'&searchType=2&searchText='
        f'&searchStartYear={year_start}&searchStartMonth={sm}&searchStartDay={sd}'
        f'&searchEndYear={year_end}&searchEndMonth={em}&searchEndDay={ed}'
        f'&listType=1&startId=zzzzz~&startPage=1&curPage=1&direction=1'
    )

    cur_url = first_url
    page = 1
    time.sleep(1.5)  # 첫 요청 전 대기 (rate-limit 방지)

    while cur_url:
        html = fetch_html(cur_url)
        if not html:
            break
        if page == 1:
            total = get_total_count(html)
            logger.info(f'  {year_start}년: 총 {total}건')

        page_reports = parse_reports(html)
        if not page_reports:
            logger.info(f'  페이지 {page}: 리포트 없음 → 수집 완료')
            break

        stop = False
        new_count = 0
        cutoff = f'{year_start}-{sm}-01'
        for date, title, pdf_url in page_reports:
            if date < cutoff:
                stop = True
                break
            att_m = re.search(r'attachmentId=(\d+)', pdf_url)
            uid = att_m.group(1) if att_m else pdf_url
            if uid not in seen_ids:
                seen_ids.add(uid)
                all_reports.append((date, title, pdf_url))
                new_count += 1

        logger.info(f'  페이지 {page}: {len(page_reports)}건 파싱, {new_count}건 추가 (누적 {len(all_reports)}건)')
        if stop:
            break

        # HTML에서 다음 페이지(page+1)의 링크 추출 (올바른 startId 커서 포함)
        next_url = find_page_link(html, page + 1)
        if not next_url:
            # rate-limit 감지: 페이지당 건수가 너무 적으면 대기 후 재시도
            if len(page_reports) < 8 and page <= 3:
                logger.warning(f'  페이지 {page}: 건수 부족({len(page_reports)}건) → rate-limit 의심, 15초 대기 후 재시도')
                time.sleep(15)
                html2 = fetch_html(cur_url)
                if html2:
                    next_url = find_page_link(html2, page + 1)
                    if next_url:
                        cur_url = next_url
                        page += 1
                        time.sleep(1.0)
                        continue
            logger.info(f'  페이지 {page}: 다음 페이지 링크 없음 → 수집 완료')
            break
        cur_url = next_url
        page += 1
        time.sleep(0.4)

    return all_reports


def collect_all_report_links(start_year=2025):
    """월별로 분할 수집 (사이트 페이지 한도 ~82페이지 우회)."""
    all_reports = []
    seen_ids = set()
    import datetime
    today = datetime.date.today()
    end_year = today.year
    end_month = today.month

    for yr in range(start_year, end_year + 1):
        last_month = end_month if yr == end_year else 12
        for mo in range(1, last_month + 1):
            import calendar
            last_day = calendar.monthrange(yr, mo)[1]
            label = f'{yr}-{mo:02d}'
            logger.info(f'[{label} 수집 시작]')
            mo_reports = collect_year_reports(yr, yr,
                                              month_start=mo, month_end=mo,
                                              day_end=last_day)
            new_count = 0
            for item in mo_reports:
                att_m = re.search(r'attachmentId=(\d+)', item[2])
                uid = att_m.group(1) if att_m else item[2]
                if uid not in seen_ids:
                    seen_ids.add(uid)
                    all_reports.append(item)
                    new_count += 1
            logger.info(f'[{label} 완료: {new_count}건 추가 / 누적 {len(all_reports)}건]')
            time.sleep(1.0)

    return all_reports


def _collect_all_report_links_legacy(start_year=2025):
    all_reports = []
    seen_ids = set()

    first_url = (
        f'{LIST_URL}?categoryId={CATEGORY_ID}'
        f'&searchType=2&searchText='
        f'&searchStartYear={start_year}&searchStartMonth=01&searchStartDay=01'
        f'&searchEndYear=2026&searchEndMonth=12&searchEndDay=31'
        f'&listType=1&startId=zzzzz~&startPage=1&curPage=1&direction=1'
    )

    cur_url = first_url
    page = 1

    while cur_url:
        html = fetch_html(cur_url)
        if not html:
            logger.error(f'페이지 {page} 빈 응답 → 중단')
            break

        if page == 1:
            total = get_total_count(html)
            logger.info(f'총 {total}건 확인. {start_year}년 이후 수집 시작...')

        page_reports = parse_reports(html)
        if not page_reports:
            logger.info(f'페이지 {page}: 리포트 없음 → 종료')
            break

        stop = False
        new_count = 0
        for date, title, pdf_url in page_reports:
            if int(date[:4]) < start_year:
                stop = True
                break
            att_m = re.search(r'attachmentId=(\d+)', pdf_url)
            uid = att_m.group(1) if att_m else pdf_url
            if uid not in seen_ids:
                seen_ids.add(uid)
                all_reports.append((date, title, pdf_url))
                new_count += 1

        logger.info(f'페이지 {page}: {len(page_reports)}건 파싱, {new_count}건 추가 (누적 {len(all_reports)}건)')

        if stop:
            logger.info(f'{start_year}년 이전 날짜 감지 → 수집 완료')
            break

        next_url = extract_next_page_url(html)
        if not next_url:
            logger.info('마지막 페이지 → 종료')
            break

        cur_url = next_url
        page += 1
        time.sleep(0.4)

    return all_reports


def main():
    ap = argparse.ArgumentParser(description='Mirae Asset report downloader')
    ap.add_argument('--save-dir', default=SAVE_DIR)
    ap.add_argument('--start-year', type=int, default=2025)
    ap.add_argument('--delay', type=float, default=0.8)
    args = ap.parse_args()

    os.makedirs(args.save_dir, exist_ok=True)
    existing = set(os.listdir(args.save_dir))
    logger.info(f'저장 경로: {args.save_dir}')
    logger.info(f'기존 파일 {len(existing)}개')

    reports = collect_all_report_links(start_year=args.start_year)
    logger.info(f'\n총 {len(reports)}개 수집 완료. 다운로드 시작...\n')

    downloaded = skipped = failed = 0
    for i, (date, title, pdf_url) in enumerate(reports, 1):
        filename = safe_filename(date, title, pdf_url)
        save_path = os.path.join(args.save_dir, filename)

        if filename in existing or os.path.exists(save_path):
            skipped += 1
            continue

        logger.info(f'[{i}/{len(reports)}] {date} | {title[:55]}')
        ok = download_pdf(pdf_url, save_path)
        if ok:
            downloaded += 1
            fsize = os.path.getsize(save_path) // 1024
            logger.info(f'  저장 완료 ({fsize}KB): {filename[:60]}')
        else:
            failed += 1
            logger.warning(f'  실패: {pdf_url}')

        time.sleep(args.delay)

    # 인덱스 CSV 저장
    index_path = os.path.join(args.save_dir, '_index.csv')
    import csv
    write_header = not os.path.exists(index_path)
    with open(index_path, 'a', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(['date', 'title', 'filename', 'pdf_url'])
        for date, title, pdf_url in reports:
            fn = safe_filename(date, title, pdf_url)
            w.writerow([date, title, fn, pdf_url])

    logger.info(f'\n=== 완료 ===')
    logger.info(f'다운로드: {downloaded} | 스킵(중복): {skipped} | 실패: {failed}')
    logger.info(f'저장 경로: {args.save_dir}')
    logger.info(f'인덱스: {index_path}')


if __name__ == '__main__':
    main()
