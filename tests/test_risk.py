import math

import numpy as np
import pandas as pd
import pytest

from mra.analytics import risk


@pytest.fixture
def normal_returns():
    return np.random.default_rng(0).normal(0.0005, 0.01, 200_000)


def test_historical_var_matches_normal_quantile(normal_returns):
    # 95% one-day VaR of N(mu, sigma) is -(mu - 1.645 sigma)
    expected = -(0.0005 - 1.6449 * 0.01)
    assert risk.historical_var(normal_returns, 0.95) == pytest.approx(expected, rel=0.02)


def test_parametric_and_historical_agree_for_normal_data(normal_returns):
    assert risk.parametric_var(normal_returns, 0.99) == pytest.approx(risk.historical_var(normal_returns, 0.99), rel=0.03)


def test_cvar_is_worse_than_var(normal_returns):
    assert risk.historical_cvar(normal_returns, 0.95) > risk.historical_var(normal_returns, 0.95)


def test_parametric_var_scales_with_sqrt_time():
    r = np.random.default_rng(1).normal(0, 0.01, 50_000)
    one, ten = risk.parametric_var(r, 0.95, 1), risk.parametric_var(r, 0.95, 10)
    assert ten / one == pytest.approx(math.sqrt(10), rel=0.05)


def test_var_ignores_nans():
    r = np.array([np.nan, -0.05, 0.01, 0.02, np.nan, -0.01])
    assert not math.isnan(risk.historical_var(r, 0.95))


def test_monte_carlo_var_close_to_analytic():
    rng = np.random.default_rng(3)
    cov = np.array([[1.0, 0.6], [0.6, 1.0]]) * 0.0001          # 1% daily vol, rho 0.6
    data = rng.multivariate_normal([0, 0], cov, 5_000)
    rets = pd.DataFrame(data, columns=["A", "B"])
    w = pd.Series({"A": 0.5, "B": 0.5})
    port_sigma = math.sqrt(w.to_numpy() @ rets.cov().to_numpy() @ w.to_numpy())
    mc = risk.monte_carlo_var(rets, w, n_sims=100_000)
    assert mc.var99 == pytest.approx(2.326 * port_sigma, rel=0.05)
    assert mc.cvar95 > mc.var95


def test_kupiec_accepts_correct_breach_rate():
    out = risk.kupiec_pof(breaches=10, observations=1000, confidence=0.99)
    assert out["lr_stat"] == pytest.approx(0.0, abs=1e-9)
    assert out["p_value"] == pytest.approx(1.0)


def test_kupiec_rejects_too_many_breaches():
    # 30 breaches where 10 are expected -> model badly underestimates risk
    assert risk.kupiec_pof(breaches=30, observations=1000, confidence=0.99)["p_value"] < 0.001


def test_kupiec_handles_zero_breaches():
    out = risk.kupiec_pof(breaches=0, observations=500, confidence=0.99)
    assert out["p_value"] < 0.05            # 0 breaches when 5 expected is also suspicious


def test_rolling_var_has_no_lookahead():
    r = pd.Series([0.0] * 300)
    r.iloc[299] = -0.5                       # a crash on the last day
    var = risk.rolling_historical_var(r, window=250, confidence=0.99)
    assert var.iloc[299] == pytest.approx(0.0)   # forecast for that day must not "see" the crash


def test_efficient_frontier_shapes():
    rets = pd.DataFrame(np.random.default_rng(5).normal(0.0004, 0.01, (500, 4)), columns=list("ABCD"))
    f = risk.efficient_frontier(rets, n_portfolios=1000)
    assert len(f) == 1000
    assert f["is_max_sharpe"].sum() == 1 and f["is_min_vol"].sum() == 1
    assert (f["volatility"] > 0).all()
