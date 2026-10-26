import pandas as pd

def align_backward(left, right, on="timestamp", tolerance_us=50_000):
    return pd.merge_asof(left.sort_values(on), right.sort_values(on),
                         on=on, direction="backward", tolerance=tolerance_us)