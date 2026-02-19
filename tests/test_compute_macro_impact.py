"""Unit tests for scripts/compute_macro_impact.py — ols_slope_t, t_to_pvalue, bh_correction."""
import sys
import os
import math

import pytest

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'scripts')
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(_SCRIPTS_DIR))

from compute_macro_impact import ols_slope_t, t_to_pvalue, bh_correction  # noqa: E402


# ---------------------------------------------------------------------------
# ols_slope_t
# ---------------------------------------------------------------------------

def test_ols_near_perfect_linear_beta_near_one():
    """y ≈ x (tiny additive noise) => beta ≈ 1.0, high |t-stat|.

    Pure y=x produces zero residuals, which makes HC3 var_beta = 0 and
    therefore se = None (t = None). We add tiny noise so residuals are
    non-zero and a finite t-statistic is produced.
    """
    import random
    random.seed(42)
    x = list(range(1, 21))
    y = [v + random.gauss(0, 0.001) for v in x]   # near-perfect, not exact
    beta, t, n = ols_slope_t(x, y)
    assert beta is not None, "beta must not be None for near-perfect linear data"
    assert abs(beta - 1.0) < 0.01, f"Expected beta≈1.0, got {beta}"
    assert t is not None, "t-statistic must not be None for near-perfect linear data"
    assert abs(t) > 10, f"Expected high |t|, got {t}"
    assert n == 20


def test_ols_exact_perfect_linear_t_is_none():
    """y = x exactly => residuals are zero => HC3 SE = 0 => t-stat is None.

    This documents the known behavior of the HC3 implementation: when all
    residuals are zero the sandwich estimator returns var_beta = 0, so se
    is set to None and t is therefore None. Beta is still correctly ≈ 1.0.
    """
    x = list(range(1, 21))
    y = list(range(1, 21))
    beta, t, n = ols_slope_t(x, y)
    assert beta is not None, "beta must not be None"
    assert abs(beta - 1.0) < 1e-6, f"Expected beta≈1.0, got {beta}"
    # t is None because HC3 SE collapses to 0 for zero-residual data
    assert t is None, f"Expected t=None for zero-residual data, got {t}"
    assert n == 20


def test_ols_too_few_observations():
    """n < 3 must return (None, None, n)."""
    beta, t, n = ols_slope_t([1, 2], [1, 2])
    assert beta is None
    assert t is None
    assert n == 2


def test_ols_single_observation():
    """n = 1 must return (None, None, 1)."""
    beta, t, n = ols_slope_t([5], [5])
    assert beta is None
    assert t is None
    assert n == 1


def test_ols_zero_variance_x():
    """All x values identical (zero variance) must return (None, None, n)."""
    x = [3.0, 3.0, 3.0, 3.0, 3.0]
    y = [1.0, 2.0, 3.0, 4.0, 5.0]
    beta, t, n = ols_slope_t(x, y)
    assert beta is None
    assert t is None
    assert n == 5


def test_ols_nan_values_are_dropped():
    """NaN entries in x or y must be dropped; remaining valid rows used."""
    x = [1, 2, float('nan'), 4, 5, 6, 7, 8, 9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    y = [1, 2, 3,            4, 5, 6, 7, 8, 9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    beta, t, n = ols_slope_t(x, y)
    # After dropping the NaN pair, 19 clean observations remain.
    assert n == 19
    assert beta is not None
    assert abs(beta - 1.0) < 0.05


def test_ols_negative_slope():
    """y = -x should produce beta ≈ -1.0."""
    x = list(range(1, 21))
    y = [-v for v in x]
    beta, t, n = ols_slope_t(x, y)
    assert beta is not None
    assert abs(beta + 1.0) < 1e-6, f"Expected beta≈-1.0, got {beta}"


# ---------------------------------------------------------------------------
# t_to_pvalue
# ---------------------------------------------------------------------------

def test_t_zero_pvalue_near_one():
    """t = 0 means no evidence against H0; p-value should be ≈ 1.0."""
    p = t_to_pvalue(0.0, 50)
    assert p is not None
    assert abs(p - 1.0) < 0.01, f"Expected p≈1.0 for t=0, got {p}"


def test_t_large_pvalue_small():
    """t = 3.5, n = 30 should give p < 0.01 (highly significant)."""
    p = t_to_pvalue(3.5, 30)
    assert p is not None
    assert p < 0.01, f"Expected p < 0.01 for t=3.5, n=30, got {p}"


def test_t_negative_same_as_positive():
    """p-value is two-tailed; t and -t must produce the same result."""
    p_pos = t_to_pvalue(2.5, 40)
    p_neg = t_to_pvalue(-2.5, 40)
    assert p_pos is not None and p_neg is not None
    assert abs(p_pos - p_neg) < 1e-10, "Two-tailed p-value must be symmetric"


def test_t_none_returns_none():
    """t = None must return None."""
    assert t_to_pvalue(None, 30) is None


def test_t_n_none_returns_none():
    """n = None must return None."""
    assert t_to_pvalue(2.0, None) is None


def test_t_nan_returns_none():
    """t = NaN must return None."""
    assert t_to_pvalue(float('nan'), 30) is None


def test_t_moderate_significance():
    """t = 2.0, n = 100 should produce a p-value in (0.01, 0.10)."""
    p = t_to_pvalue(2.0, 100)
    assert p is not None
    assert 0.01 < p < 0.10, f"Expected p in (0.01, 0.10), got {p}"


# ---------------------------------------------------------------------------
# bh_correction
# ---------------------------------------------------------------------------

def test_bh_adjusted_ge_raw():
    """BH-adjusted p-values must be >= the corresponding raw p-values."""
    raw = [0.001, 0.01, 0.05, 0.1, 0.5]
    adj = bh_correction(raw)
    assert len(adj) == len(raw)
    for r, a in zip(raw, adj):
        assert a is not None
        assert a >= r - 1e-12, f"Adjusted {a} must be >= raw {r}"


def test_bh_none_entries_preserved():
    """None entries in the input must remain None in the output."""
    raw = [None, 0.01, None, 0.05, 0.5]
    adj = bh_correction(raw)
    assert len(adj) == len(raw)
    assert adj[0] is None, "First entry (None) must remain None"
    assert adj[2] is None, "Third entry (None) must remain None"
    # Non-None entries must be floats
    assert isinstance(adj[1], float)
    assert isinstance(adj[3], float)
    assert isinstance(adj[4], float)


def test_bh_all_none_returns_all_none():
    """All-None input must return all-None output."""
    adj = bh_correction([None, None, None])
    assert adj == [None, None, None]


def test_bh_single_pvalue():
    """Single p-value adjusted equals itself (capped at 1.0)."""
    adj = bh_correction([0.03])
    assert len(adj) == 1
    assert adj[0] is not None
    assert abs(adj[0] - 0.03) < 1e-10


def test_bh_output_capped_at_one():
    """No adjusted p-value should exceed 1.0."""
    raw = [0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    adj = bh_correction(raw)
    for a in adj:
        if a is not None:
            assert a <= 1.0, f"Adjusted p-value {a} exceeds 1.0"


def test_bh_length_matches_input():
    """Output length must equal input length."""
    raw = [0.001, 0.01, 0.05, 0.1, 0.5]
    adj = bh_correction(raw)
    assert len(adj) == len(raw)


def test_bh_monotone_nondecreasing_for_sorted_input():
    """For already-sorted raw p-values the BH-adjusted values must be
    monotonically non-decreasing (enforced by the backward-pass in BH).
    """
    raw = [0.001, 0.005, 0.01, 0.05, 0.1]
    adj = bh_correction(raw)
    for i in range(len(adj) - 1):
        if adj[i] is not None and adj[i + 1] is not None:
            assert adj[i] <= adj[i + 1] + 1e-12, (
                f"BH adjusted values not non-decreasing: adj[{i}]={adj[i]} > adj[{i+1}]={adj[i+1]}"
            )
