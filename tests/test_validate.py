import numpy as np
import pandas as pd
import pytest
from ftpipe.validate import qc_topic

@pytest.fixture
def clean_20hz():
    t = np.arange(0, 10_000_000, 50_000)          # 10 s at exactly 20 Hz, in µs
    return pd.DataFrame({"timestamp": t, "x": np.zeros(len(t))})

def test_clean_signal_has_no_findings(clean_20hz):
    r = qc_topic(clean_20hz)
    assert r["n_samples"] == 200
    assert r["rate_hz_median"] == pytest.approx(20.0)
    assert r["n_gaps_3x"] == 0
    assert r["n_nan"] == 0
    assert r["duration_s"] == pytest.approx(9.95)

def test_detects_one_gap(clean_20hz):
    df = clean_20hz.drop(index=range(100, 110))   # cut out 10 samples = one 0.55 s hole
    assert qc_topic(df)["n_gaps_3x"] == 1

def test_counts_nans(clean_20hz):
    df = clean_20hz.copy()
    df.loc[[3, 7], "x"] = np.nan                  # exactly two NaNs
    assert qc_topic(df)["n_nan"] == 2