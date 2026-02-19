#!/usr/bin/env python3
"""Pipeline-wide default constants for MACRO-DATA.

Centralises magic numbers that are currently scattered across nightly.yml
CLI arguments. Scripts and the workflow file should reference these values
so that a single change here propagates everywhere.

Usage example:
    from config_defaults import CYCLE_WINDOWS, ANALYSIS_WINDOWS, LAST_DAYS, LAST_EVENTS
    parser.add_argument('--cycle-windows', default=','.join(map(str, CYCLE_WINDOWS)))
"""

# ── Momentum / cycle lookback windows (trading days) ──────────────────────────
# Used by: build_reaction_matrix.py (--cycle-windows), join_cycles_into_reactions.py
CYCLE_WINDOWS: list[int] = [21, 63, 126]   # ~1 month, 3 months, 6 months

# ── Additional return windows (trading days, half-window) ─────────────────────
# Used by: compute_additional_windows.py (--windows)
ANALYSIS_WINDOWS: list[int] = [5, 10, 21]  # ±5d, ±10d, ±21d

# ── Event filter defaults ──────────────────────────────────────────────────────
# Used by: compute_reaction_by_surprise_quantile.py, analyze_focus_events.py
#           build_excel_dashboard_plus.py
LAST_DAYS: int = 180        # rolling window for recent-event analysis
LAST_EVENTS: int = 100      # max events to include in focus/quantile sheets
DASHBOARD_LAST_DAYS: int = 365    # wider window for dashboard overview
DASHBOARD_LAST_EVENTS: int = 180  # max events in full dashboard

# ── Surprise z-score rolling windows (observations, not days) ─────────────────
# Used by: utils_surprise.py
# Values must match _FREQ_WINDOW in utils_surprise.py
SURPRISE_WINDOW_MONTHLY: int = 12   # 12 monthly obs ≈ 1 year
SURPRISE_WINDOW_WEEKLY: int = 26    # 26 weekly obs ≈ half year
SURPRISE_WINDOW_QUARTERLY: int = 8  # 8 quarterly obs ≈ 2 years
SURPRISE_MIN_PERIODS: int = 3       # minimum obs before rolling std is valid

# ── Statistical thresholds ────────────────────────────────────────────────────
FDR_ALPHA: float = 0.05             # Benjamini-Hochberg significance level
MIN_SAMPLE_REGRESSION: int = 5      # minimum n for OLS / asymmetry regressions

# ── Data fetch parameters ─────────────────────────────────────────────────────
FETCH_START_DATE: str = '2000-01-01'
FMP_TIMEOUT_SEC: int = 25
BOK_TIMEOUT_SEC: int = 20
FRED_TIMEOUT_SEC: int = 20
MAX_RETRY_ATTEMPTS: int = 5
PRICE_COVERAGE_WARN_THRESHOLD: float = 0.30  # warn if <30% of expected trading days fetched
