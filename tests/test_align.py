import pandas as pd
from ftpipe.align import align_backward

meas = pd.DataFrame({"timestamp": [0, 5, 10, 15], "y": [0.0] * 4})
sp   = pd.DataFrame({"timestamp": [0, 12], "cmd": [1.0, 2.0]})

def test_is_causal():
    m = align_backward(meas, sp, tolerance_us=100)
    assert m["cmd"].tolist() == [1.0, 1.0, 1.0, 2.0]   # t=10 must NOT see the t=12 command

def test_stale_setpoint_is_masked():
    m = align_backward(meas, sp, tolerance_us=4)
    assert m["cmd"].isna().tolist() == [False, True, True, False]