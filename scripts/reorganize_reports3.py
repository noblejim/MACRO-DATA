"""
reorganize_reports3.py - Phase 3
- _uncategorized/ 내 YYYYMMDD_ 형식 파일들도 YYYY/MM/ 로 이동
- 날짜 없는 기타 파일은 _uncategorized/misc/ 로 이동
"""
import os, re, shutil, glob

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE, 'reports')
UNCAT_DIR = os.path.join(REPORTS_DIR, '_uncategorized')
ARCHIVE_BEFORE = 2025

moved = 0
errors = []

for fname in glob.glob(os.path.join(UNCAT_DIR, '**', '*.pdf'), recursive=True):
    base = os.path.basename(fname)
    # pattern 1: YYYY-MM-DD_
    m = re.match(r'^(\d{4})-(\d{2})-\d{2}_', base)
    # pattern 2: YYYYMMDD_
    m2 = re.match(r'^(\d{4})(\d{2})\d{2}_', base)

    if m:
        year, month = m.group(1), m.group(2)
    elif m2:
        year, month = m2.group(1), m2.group(2)
    else:
        # move to misc
        misc_dir = os.path.join(UNCAT_DIR, 'misc')
        os.makedirs(misc_dir, exist_ok=True)
        try:
            shutil.move(fname, os.path.join(misc_dir, base))
        except Exception as e:
            errors.append(str(e))
        continue

    if int(year) < ARCHIVE_BEFORE:
        dest_dir = os.path.join(REPORTS_DIR, '_archive', year, month)
    else:
        dest_dir = os.path.join(REPORTS_DIR, year, month)
    os.makedirs(dest_dir, exist_ok=True)

    dst = os.path.join(dest_dir, base)
    if os.path.exists(dst):
        stem, ext = os.path.splitext(base)
        dst = os.path.join(dest_dir, stem + '_dup' + ext)
    try:
        shutil.move(fname, dst)
        moved += 1
    except Exception as e:
        errors.append(f"{base}: {e}")

print(f"Moved: {moved} files from _uncategorized")

# Clean up empty dirs in _uncategorized
for dirpath, dirnames, filenames in os.walk(UNCAT_DIR, topdown=False):
    if not os.listdir(dirpath):
        os.rmdir(dirpath)

# Check what's left
remaining = list(glob.glob(os.path.join(UNCAT_DIR, '**', '*'), recursive=True))
print(f"Remaining in _uncategorized: {len(remaining)} items")
for r in remaining[:10]:
    print(f"  {os.path.basename(r)}")
if errors:
    for e in errors[:5]:
        print(f"ERROR: {e}")
print("Done.")
