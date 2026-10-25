from pyulog import ULog
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

ulog = ULog("F2.ulg")

att = ulog.get_dataset("vehicle_attitude")

print(ulog.dropouts)
print(list(ulog.initial_parameters.items())[:5])

data = pd.DataFrame(att.data)

dt = data["timestamp"].diff()
print(1e6 / dt.median(), "Hz nominal")
print("gaps > 3x nominal:", (dt > 3 * dt.median()).sum())
print(data.isna().sum())
print(data.describe())

datasets = ["vehicle_attitude", "vehicle_angular_velocity", "vehicle_rates_setpoint", "actuator_servos", "airspeed_validated",]
for dataset in datasets:
    att = ulog.get_dataset(dataset)
    data = pd.DataFrame(att.data)
    table = pa.Table.from_pandas(data)
    out = Path("data") / f"topic={dataset}" / f"flight_id={2}" / "data.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, ('data/topic='+dataset+'/flight_id=2/data.parquet'))