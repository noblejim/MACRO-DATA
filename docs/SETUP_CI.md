# CI Setup (MACRO-DATA)

This repository includes a Nightly GitHub Actions workflow (`.github/workflows/nightly.yml`) scheduled daily at 02:00 KST (17:00 UTC) and manually runnable via the Actions tab.

## 1) Add Repository Secrets

Go to GitHub → Settings → Secrets and variables → Actions → New repository secret, then add:
- `FMP_API_KEY` (required for US prices via FMP)
- `FRED_API_KEY` (required for US macro via FRED)
- `BOK_API_KEY` (optional, KR macro via ECOS)
- `KOSIS_API_KEY` (optional, KR macro via KOSIS)

## 2) What’s in this PR

- Minimal workflow that verifies the environment (Python 3.11) — a smoke test to confirm Actions runs in this repo.
- `.gitignore` tuned for this project (ignores `out/**`, Excel dashboards, local caches, and logs).
- Placeholders for `dashboards/` and `out/` so the folders exist in a clean clone.

## 3) Next PR (full pipeline)

We will push the full `scripts/`, `data/us|kr` (templates and mappings), and docs to enable the pipeline:
- US: FRED fetch, merge, reactions/impact, focus reports, and dashboard build
- KR: BOK/KOSIS fetch (when keys available), merge, reactions/impact, and dashboard build

The nightly workflow will then:
1. Set up Python + dependencies (pandas, numpy, xlsxwriter, etc.)
2. Run the pipeline scripts (US first; KR optional)
3. Upload generated dashboards and CSV outputs as artifacts

## 4) Manual run

Once secrets are set and pipeline files are pushed, trigger a manual run:
- Actions → nightly → Run workflow → Run

Check the run logs for progress and artifacts (`dashboards` and `outputs`).
