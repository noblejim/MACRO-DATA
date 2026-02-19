# Fix broken Korean filenames in downloaded Mirae Asset reports
import os, re, gzip, time, csv
from urllib.request import urlopen, Request

FOLDER = os.path.join(os.path.expanduser('~'), 'Documents', 'MACRO-DATA', '\uacbd\uc81c \ub9ac\ud3ec\ud2b8')
BASE = 'https://securities.miraeasset.com'
LIST = BASE + '/bbs/board/message/list.do'


def fetch(url):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'ko-KR', 'Accept-Encoding': 'gzip,deflate'})
    with urlopen(req, timeout=20) as r:
        raw = r.read()
        enc = r.headers.get('Content-Encoding', '')
    if enc == 'gzip':
        try: raw = gzip.decompress(raw)
        except: pass
    return raw.decode('euc-kr', errors='ignore')


def build_url(href):
    href = href.replace('&amp;', '&')
    if href.startswith('http'): return href
    if href.startswith('/'): return BASE + href
    return BASE + '/bbs/board/message/' + href


def collect_all():
    url = (LIST + '?categoryId=1521&searchType=2&searchText='
           '&searchStartYear=2025&searchStartMonth=01&searchStartDay=01'
           '&searchEndYear=2026&searchEndMonth=12&searchEndDay=31'
           '&listType=1&startId=zzzzz~&startPage=1&curPage=1&direction=1')
    all_reports = []
    page = 1
    while url:
        html = fetch(url)
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
        page_rpts = []
        for row in rows:
            date_m = re.search(r'(202\d-\d{2}-\d{2})', row)
            if not date_m: continue
            title_m = re.search(r'id="bbsTitle\d+"[^>]*>(.*?)</a>', row, re.DOTALL | re.IGNORECASE)
            if not title_m: continue
            title = re.sub(r'<[^>]+>', '', title_m.group(1)).replace('\n', ' ').strip()
            title = re.sub(r'\s+', ' ', title)
            pdf_m = re.search(r"downConfirm\('(https?://[^']+\.pdf[^']*)'", row)
            if not pdf_m: continue
            pdf_url = re.sub(r'(attachmentId=\d+).*', r'\1', pdf_m.group(1).strip())
            page_rpts.append((date_m.group(1), title, pdf_url))
        all_reports.extend(page_rpts)
        cur_m = re.search(r'class="on"[^>]*>(\d+)<', html)
        cur = int(cur_m.group(1)) if cur_m else page
        links = re.findall(r'href="(list\.do\?[^"]+curPage=(\d+)[^"]*)"', html)
        next_url = None
        for href, pg in links:
            if int(pg) == cur + 1:
                next_url = build_url(href)
                break
        if not next_url and links:
            cands = sorted([(int(p), h) for h, p in links])
            if cands and cands[-1][0] > cur:
                next_url = build_url(cands[-1][1])
        if not next_url or not page_rpts:
            break
        url = next_url
        page += 1
        time.sleep(0.3)
    return all_reports


def safe_title(title):
    s = re.sub(r'[\\/:*?"<>|]', '_', title)
    return re.sub(r'\s+', ' ', s).strip()[:70]


def main():
    print(f'링크 수집 중...')
    reports = collect_all()
    print(f'총 {len(reports)}개 수집')

    # attachmentId → (date, title, pdf_url) 매핑
    id_map = {}
    for date, title, pdf_url in reports:
        att_m = re.search(r'attachmentId=(\d+)', pdf_url)
        if att_m:
            id_map[att_m.group(1)] = (date, title, pdf_url)

    # 깨진 파일명 리네이밍
    renamed = 0
    for fn in list(os.listdir(FOLDER)):
        if not fn.endswith('.pdf'):
            continue
        if not any(ord(c) >= 0xFFFD for c in fn):
            continue  # 정상 파일
        att_m = re.search(r'_(\d{7,})\.pdf$', fn)
        if not att_m:
            continue
        att_id = att_m.group(1)
        if att_id not in id_map:
            continue
        date, title, pdf_url = id_map[att_id]
        new_fn = f'{date}_{safe_title(title)}_{att_id}.pdf'
        old_path = os.path.join(FOLDER, fn)
        new_path = os.path.join(FOLDER, new_fn)
        if old_path != new_path and not os.path.exists(new_path):
            os.rename(old_path, new_path)
            renamed += 1
            print(f'  리네임: {new_fn[:70]}')

    print(f'\n리네이밍 완료: {renamed}개')

    # 인덱스 CSV 저장
    index_path = os.path.join(FOLDER, '_index.csv')
    with open(index_path, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date', 'title', 'pdf_url'])
        for date, title, pdf_url in sorted(reports, key=lambda x: x[0], reverse=True):
            w.writerow([date, title, pdf_url])
    print(f'인덱스 저장: {index_path} ({len(reports)}행)')

    # 최종 현황
    pdfs = [f for f in os.listdir(FOLDER) if f.endswith('.pdf')]
    good = sum(1 for f in pdfs if not any(ord(c) >= 0xFFFD for c in f))
    print(f'\n최종: 총 {len(pdfs)}개 PDF, 정상파일명 {good}개')


if __name__ == '__main__':
    main()
