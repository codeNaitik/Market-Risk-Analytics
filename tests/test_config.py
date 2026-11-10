import pytest
import yaml

from mra import config


def test_settings_load_and_paths(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", "abfss://lake@acct.dfs.core.windows.net")
    config.get_settings.cache_clear()
    s = config.get_settings()
    assert s.is_cloud
    assert s.path("bronze", "fred") == "abfss://lake@acct.dfs.core.windows.net/bronze/fred"
    assert set(s.portfolio["weights"]) <= set(s.ticker_symbols)
    config.get_settings.cache_clear()


def test_weights_must_sum_to_one(tmp_path, monkeypatch):
    raw = yaml.safe_load(config.DEFAULT_SETTINGS.read_text())
    raw["portfolio"]["weights"]["AAPL"] += 0.05
    bad = tmp_path / "settings.yaml"
    bad.write_text(yaml.safe_dump(raw))
    config.get_settings.cache_clear()
    with pytest.raises(ValueError, match="sum to 1.0"):
        config.get_settings(str(bad))
    config.get_settings.cache_clear()
