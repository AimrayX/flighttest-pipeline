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

rows = []
for f in sorted(Path("data").glob("topic=*/flight_id=*/data.parquet")):
    topic = f.parent.parent.name.split("=")[1]
    flight = f.parent.name.split("=")[1]
    df = pd.read_parquet(f)
    rows.append({"topic": topic, "flight_id": flight, **qc_topic(df)})

qc = pd.DataFrame(rows)
print(qc.to_string())
qc.to_csv("qc_report.csv", index=False)