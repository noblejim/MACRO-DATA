"""
reorganize_reports2.py - Phase 2
- reports/ 내 하위 카테고리 폴더들의 PDF를 YYYY/MM/ 계층으로 이동
- reports/ 내 .md 파일을 docs/로 이동
"""
import os, re, shutil

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE, 'reports')
DOCS_DIR = os.path.join(BASE, 'docs')
ARCHIVE_BEFORE = 2025

# Category subfolders to flatten
CATEGORY_DIRS = [
    '글로벌 시장 브리핑', '글로벌 이슈', '산업분석', '섹터_테마 리포트',
    '자산배분_전략', '종목 리포트', '주간_월간 정기물', '투자전략',
    'Fixed Income', 'SMR', '2025 전망', '2026 전망',
]

moved = 0
errors = []

for cat in CATEGORY_DIRS:
    cat_path = os.path.join(REPORTS_DIR, cat)
    if not os.path.isdir(cat_path):
        continue
    pdfs = [f for f in os.listdir(cat_path) if f.lower().endswith('.pdf')]
    for fname in pdfs:
        m = re.match(r'^(\d{4})-(\d{2})-\d{2}_', fname)
        if not m:
            # no date prefix - put in _uncategorized
            dest_dir = os.path.join(REPORTS_DIR, '_uncategorized', cat)
        else:
            year, month = m.group(1), m.group(2)
            if int(year) < ARCHIVE_BEFORE:
                dest_dir = os.path.join(REPORTS_DIR, '_archive', year, month)
            else:
                dest_dir = os.path.join(REPORTS_DIR, year, month)
        os.makedirs(dest_dir, exist_ok=True)
        src = os.path.join(cat_path, fname)
        dst = os.path.join(dest_dir, fname)
        if os.path.exists(dst):
            stem, ext = os.path.splitext(fname)
            dst = os.path.join(dest_dir, stem + '_dup' + ext)
        try:
            shutil.move(src, dst)
            moved += 1
        except Exception as e:
            errors.append(f"{fname}: {e}")
    # remove empty dir
    try:
        os.rmdir(cat_path)
        print(f"  Removed empty dir: {cat}")
    except:
        remaining = os.listdir(cat_path)
        print(f"  Dir not empty ({len(remaining)} items): {cat}")

print(f"Moved: {moved} files from category folders")

# Move .md files from reports/ root to docs/
for fname in os.listdir(REPORTS_DIR):
    if fname.endswith('.md'):
        src = os.path.join(REPORTS_DIR, fname)
        dst = os.path.join(DOCS_DIR, fname)
        shutil.move(src, dst)
        print(f"Moved to docs/: {fname}")

if errors:
    for e in errors[:10]:
        print(f"ERROR: {e}")
print("Done.")
