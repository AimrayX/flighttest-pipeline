from pathlib import Path
import pandas as pd
from ftpipe.decode import decode_ulog, TOPICS
from ftpipe.store import write_topic
from ftpipe.validate import qc_topic

RAW, DATA = Path("raw"), Path("data")

def main():
    rows = []
    for ulg in sorted(RAW.glob("*.ulg")):
        flight_id = ulg.stem
        topics = decode_ulog(ulg)
        missing = set(TOPICS) - topics.keys()
        if missing:
            print(f"{flight_id}: missing topics {sorted(missing)}")
        for name, df in topics.items():
            write_topic(df, DATA, name, flight_id)
            rows.append({"topic": name, "flight_id": flight_id, **qc_topic(df)})
    pd.DataFrame(rows).to_csv("qc_report.csv", index=False)

if __name__ == "__main__":
    main()