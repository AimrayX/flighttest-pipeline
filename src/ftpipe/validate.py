from pathlib import Path
import pandas as pd

def qc_topic(df) -> dict:
    dt = df["timestamp"].diff().dropna()
    return {
        "n_samples": len(df),
        "rate_hz_median": 1e6 / dt.median(),
        "dt_p99_ms": dt.quantile(.99) / 1e3,
        "n_gaps_3x": int((dt > 3 * dt.median()).sum()),
        "n_nan": int(df.isna().sum().sum()),
        "duration_s": (df["timestamp"].iloc[-1] - df["timestamp"].iloc[0]) / 1e6,
    }