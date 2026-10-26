import pytest
import pandas as pd
from ftpipe.align import align_backward

@pytest.fixture
def meas():
    return pd.DataFrame({"timestamp": [0, 5, 10, 15], "y": [0.0]*4})

@pytest.fixture
def sp():
    return pd.DataFrame({"timestamp": [0, 12], "cmd": [1.0, 2.0]})

def test_is_causal(meas, sp):
    m = align_backward(meas, sp, tolerance_us=100)
    assert m["cmd"].tolist() == [1.0, 1.0, 1.0, 2.0]

def test_stale_setpoint_is_masked(meas, sp):
    m = align_backward(meas, sp, tolerance_us=4)
    assert m["cmd"].isna().tolist() == [False, True, True, False]