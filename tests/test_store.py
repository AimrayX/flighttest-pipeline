import pandas as pd
import pytest
from ftpipe.store import write_topic

@pytest.fixture
def df():
    return pd.DataFrame({"timestamp": [0, 5, 10, 15], "y": [0.0] * 4})

def test_writes_to_partitioned_path(df, tmp_path):
    out = write_topic(df, tmp_path, "vehicle_attitude", "test_flight0")

    assert out == tmp_path / "topic=vehicle_attitude" / "flight_id=test_flight0" / "data.parquet"
    assert out.exists()
    assert pd.read_parquet(out).equals(df)    # round trip: same data comes back