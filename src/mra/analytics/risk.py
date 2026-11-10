"""Portfolio risk maths (pure NumPy/pandas, unit-tested in tests/test_risk.py).

Covers the methods a market-risk team uses day to day:
  * Value at Risk three ways: historical, parametric (variance-covariance), Monte Carlo
  * Expected Shortfall (CVaR)
  * VaR backtesting with Kupiec's proportion-of-failures (POF) test
  * Long-only efficient frontier by random portfolio sampling

Conventions: returns are simple daily returns (0.01 = +1%). VaR and CVaR are returned
as POSITIVE loss fractions (0.02 = a 2% loss), which is how risk reports state them.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

Z_SCORES = {0.95: 1.6448536269514722, 0.99: 2.3263478740408408}


# --------------------------------------------------------------------------- VaR / CVaR
def historical_var(returns: np.ndarray, confidence: float = 0.95) -> float:
    """Loss not exceeded on `confidence` share of historical days."""
    r = np.asarray(returns, dtype=float)
    r = r[~np.isnan(r)]
    return float(-np.quantile(r, 1 - confidence))


def historical_cvar(returns: np.ndarray, confidence: float = 0.95) -> float:
    """Average loss on the days worse than the VaR threshold (Expected Shortfall)."""
    r = np.asarray(returns, dtype=float)
    r = r[~np.isnan(r)]
    threshold = np.quantile(r, 1 - confidence)
    return float(-r[r <= threshold].mean())


def parametric_var(returns: np.ndarray, confidence: float = 0.95, horizon_days: int = 1) -> float:
    """Variance-covariance VaR assuming normal returns, scaled by sqrt(time)."""
    r = np.asarray(returns, dtype=float)
    r = r[~np.isnan(r)]
    mu, sigma = r.mean(), r.std(ddof=1)
    return float(-(mu * horizon_days - Z_SCORES[confidence] * sigma * math.sqrt(horizon_days)))


@dataclass
class MonteCarloResult:
    var95: float
    var99: float
    cvar95: float
    horizon_days: int
    n_sims: int
    simulated_returns: np.ndarray


def monte_carlo_var(asset_returns: pd.DataFrame, weights: pd.Series, n_sims: int = 20_000,
                    horizon_days: int = 1, seed: int = 7) -> MonteCarloResult:
    """Simulate correlated asset returns from a multivariate normal (Cholesky of the
    covariance matrix), compound them over the horizon, and read VaR off the portfolio
    P&L distribution."""
    rets = asset_returns[weights.index].dropna()
    mu = rets.mean().to_numpy()
    cov = rets.cov().to_numpy()
    chol = np.linalg.cholesky(cov + np.eye(len(mu)) * 1e-12)          # jitter keeps it positive-definite
    rng = np.random.default_rng(seed)

    shocks = rng.standard_normal((n_sims, horizon_days, len(mu)))
    daily = mu + shocks @ chol.T                                       # (sims, days, assets)
    asset_horizon = np.prod(1 + daily, axis=1) - 1                     # compound per asset
    port = asset_horizon @ weights.to_numpy()

    return MonteCarloResult(
        var95=historical_var(port, 0.95), var99=historical_var(port, 0.99),
        cvar95=historical_cvar(port, 0.95), horizon_days=horizon_days, n_sims=n_sims,
        simulated_returns=port,
    )


# --------------------------------------------------------------------------- backtesting
def rolling_historical_var(returns: pd.Series, window: int = 252, confidence: float = 0.99) -> pd.Series:
    """VaR forecast for day t uses only days t-window .. t-1 (no look-ahead)."""
    return -returns.shift(1).rolling(window).quantile(1 - confidence)


def kupiec_pof(breaches: int, observations: int, confidence: float) -> dict[str, float]:
    """Kupiec (1995) proportion-of-failures test.

    H0: the observed breach rate equals the expected rate (1 - confidence).
    LR ~ chi-squared(1); p < 0.05 means the VaR model is mis-calibrated.
    """
    p = 1 - confidence
    x, t = breaches, observations
    if t == 0:
        return {"lr_stat": float("nan"), "p_value": float("nan"), "expected_breaches": 0.0}
    phat = x / t

    def _ll(prob: float) -> float:
        # log-likelihood with 0*log(0) := 0
        a = (t - x) * math.log(1 - prob) if prob < 1 else (0.0 if x == t else -math.inf)
        b = x * math.log(prob) if prob > 0 else (0.0 if x == 0 else -math.inf)
        return a + b

    lr = -2 * (_ll(p) - _ll(phat))
    p_value = math.erfc(math.sqrt(max(lr, 0) / 2))                     # chi2(1) survival function
    return {"lr_stat": lr, "p_value": p_value, "expected_breaches": p * t}


def var_backtest(returns: pd.Series, window: int = 252, confidence: float = 0.99) -> tuple[pd.DataFrame, dict]:
    var = rolling_historical_var(returns, window, confidence)
    df = pd.DataFrame({"portfolio_return": returns, "var_forecast": var}).dropna()
    df["is_breach"] = df["portfolio_return"] < -df["var_forecast"]
    summary = {"confidence": confidence, "window": window, "observations": len(df),
               "breaches": int(df["is_breach"].sum()), **kupiec_pof(int(df["is_breach"].sum()), len(df), confidence)}
    summary["model_ok_at_5pct"] = bool(summary["p_value"] >= 0.05)
    return df, summary


# --------------------------------------------------------------------------- frontier
def efficient_frontier(asset_returns: pd.DataFrame, n_portfolios: int = 5_000, risk_free: float = 0.0,
                       trading_days: int = 252, seed: int = 11) -> pd.DataFrame:
    """Sample long-only portfolios (Dirichlet weights) and compute annualised risk/return."""
    rets = asset_returns.dropna()
    mu = rets.mean().to_numpy() * trading_days
    cov = rets.cov().to_numpy() * trading_days
    rng = np.random.default_rng(seed)
    w = rng.dirichlet(np.ones(len(mu)), n_portfolios)
    port_ret = w @ mu
    port_vol = np.sqrt(np.einsum("ij,jk,ik->i", w, cov, w))
    out = pd.DataFrame({"expected_return": port_ret, "volatility": port_vol})
    out["sharpe_ratio"] = (out["expected_return"] - risk_free) / out["volatility"]
    out["weights_json"] = [
        "{" + ",".join(f'"{t}":{x:.4f}' for t, x in zip(rets.columns, row, strict=False)) + "}" for row in w
    ]
    out["is_max_sharpe"] = out["sharpe_ratio"] == out["sharpe_ratio"].max()
    out["is_min_vol"] = out["volatility"] == out["volatility"].min()
    return out
