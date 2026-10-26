from pathlib import Path
import pandas as pd
from pyulog import ULog

TOPICS = ["vehicle_attitude", "vehicle_angular_velocity", "vehicle_rates_setpoint",
          "actuator_servos", "airspeed_validated", "vehicle_status", "navigator_status"]

def decode_ulog(path: Path, topics: list[str] = TOPICS) -> dict[str, pd.DataFrame]:
    ulog = ULog(str(path), message_name_filter_list=topics)
    out = {}
    for d in ulog.data_list:
        if d.multi_id != 0:
            continue
        df = pd.DataFrame(d.data)
        for col in ("timestamp", "timestamp_sample"):
            if col in df:
                df[col] = df[col].astype("int64")   # avoid uint64 underflow downstream
        out[d.name] = df
    return out