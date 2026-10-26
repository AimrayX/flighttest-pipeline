from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def write_topic(df: pd.DataFrame, DATA: Path, name: str, flight_id: str) -> Path:
    table = pa.Table.from_pandas(df, preserve_index=False)
    out = DATA / f"topic={name}" / f"flight_id={flight_id}" / "data.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(out))
    return out
