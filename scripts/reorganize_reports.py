"""
reorganize_reports.py
- 경제 리포트 폴더를 reports/로 이름 변경
- reports/ 루트 PDF를 YYYY/MM/ 계층으로 이동
- 2022-2024 파일은 _archive/YYYY/MM/ 로 이동
- _index.csv 경로 업데이트
- 파일명 / -> - 치환, 제목 80자 이하 truncate
"""
import os
import re
import shutil
import csv
import sys

BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '')
OLD_DIR = os.path.join(BASE, '경제 리포트')
NEW_DIR = os.path.join(BASE, 'reports')
INDEX_PATH = None  # will be set after rename

ARCHIVE_BEFORE_YEAR = 2025  # 2022-2024 → _archive/

moved = 0
renamed = 0
errors = []

def sanitize_filename(name):
    """/ → -, 제목 부분 80자 이하 truncate"""
    # name without extension
    stem, ext = os.path.splitext(name)
    # replace / with -
    new_stem = stem.replace('/', '-')
    # check length: date(10) + _ + title + _ + id(7)
    # format: YYYY-MM-DD_title_NNNNNNN
    parts = new_stem.split('_')
    if len(parts) >= 3 and re.match(r'\d{4}-\d{2}-\d{2}', parts[0]):
        date_part = parts[0]
        id_part = parts[-1]
        title_parts = parts[1:-1]
        title = '_'.join(title_parts)
        if len(title) > 80:
            title = title[:80]
        new_stem = f"{date_part}_{title}_{id_part}"
    return new_stem + ext, new_stem != stem

# Step 1: rename 경제 리포트 → reports
if os.path.exists(OLD_DIR) and not os.path.exists(NEW_DIR):
    os.rename(OLD_DIR, NEW_DIR)
    print(f"Renamed: 경제 리포트 → reports")
elif os.path.exists(NEW_DIR):
    print(f"reports/ already exists, skipping rename")
else:
    print(f"ERROR: Neither old nor new dir found")
    sys.exit(1)

INDEX_PATH = os.path.join(NEW_DIR, '_index.csv')

# Step 2: collect all PDFs in reports/ root (not in subdirs)
root_pdfs = [f for f in os.listdir(NEW_DIR)
             if f.lower().endswith('.pdf') and os.path.isfile(os.path.join(NEW_DIR, f))]
print(f"Found {len(root_pdfs)} PDFs in reports/ root")

# Step 3: move each PDF to YYYY/MM/ or _archive/YYYY/MM/
for fname in root_pdfs:
    # extract date from filename YYYY-MM-DD_...
    m = re.match(r'^(\d{4})-(\d{2})-\d{2}_', fname)
    if not m:
        print(f"  SKIP (no date): {fname}")
        continue
    year, month = m.group(1), m.group(2)

    # sanitize filename
    new_fname, was_renamed = sanitize_filename(fname)
    if was_renamed:
        renamed += 1

    # determine dest
    if int(year) < ARCHIVE_BEFORE_YEAR:
        dest_dir = os.path.join(NEW_DIR, '_archive', year, month)
    else:
        dest_dir = os.path.join(NEW_DIR, year, month)

    os.makedirs(dest_dir, exist_ok=True)
    src = os.path.join(NEW_DIR, fname)
    dst = os.path.join(dest_dir, new_fname)

    if os.path.exists(dst):
        # avoid overwrite collision
        base_stem, ext = os.path.splitext(new_fname)
        dst = os.path.join(dest_dir, base_stem + '_dup' + ext)

    try:
        shutil.move(src, dst)
        moved += 1
    except Exception as e:
        errors.append(f"{fname}: {e}")

print(f"Moved: {moved} files, Renamed: {renamed} files")
if errors:
    print(f"Errors ({len(errors)}):")
    for e in errors[:10]:
        print(f"  {e}")

# Step 4: update _index.csv
if os.path.exists(INDEX_PATH):
    updated = 0
    rows = []
    with open(INDEX_PATH, encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            fpath = row.get('filepath', '') or row.get('file_path', '') or row.get('path', '')
            # find which column is the path
            path_col = None
            for col in (reader.fieldnames or []):
                if 'path' in col.lower() or 'file' in col.lower():
                    path_col = col
                    break
            rows.append(row)

    # Re-detect path column
    path_col = None
    if fieldnames:
        for col in fieldnames:
            if 'path' in col.lower() or col.lower() in ('filename', 'file'):
                path_col = col
                break

    if path_col:
        for row in rows:
            old_path = row[path_col]
            # update 경제 리포트 → reports in path
            new_path = old_path.replace('경제 리포트', 'reports').replace('경제 리포트', 'reports')
            if new_path != old_path:
                row[path_col] = new_path
                updated += 1

        with open(INDEX_PATH, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"_index.csv updated: {updated} path entries changed")
    else:
        print("_index.csv: no path column found, skipping update")
else:
    print("_index.csv not found")

print("\nDone.")
