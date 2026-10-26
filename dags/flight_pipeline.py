from pathlib import Path
from airflow.sdk import dag, task

PROJECT = Path(__file__).resolve().parent.parent
RAW, DATA = PROJECT / "raw", PROJECT / "data"

@dag(schedule=None, tags=["flighttest"])
def flight_pipeline():

    @task
    def list_flights() -> list[str]:
        return [str(p) for p in sorted(RAW.glob("*.ulg"))]

    @task
    def ingest(ulg_path: str) -> dict:
        from ftpipe.decode import decode_ulog
        from ftpipe.store import write_topic
        ulg = Path(ulg_path)
        paths = {name: str(write_topic(df, DATA, name, ulg.stem))
                 for name, df in decode_ulog(ulg).items()}
        return {"flight_id": ulg.stem, "paths": paths}

    @task
    def validate(ingested: dict) -> list[dict]:
        import pandas as pd
        from ftpipe.validate import qc_topic
        return [{"topic": t, "flight_id": ingested["flight_id"], **qc_topic(pd.read_parquet(p))}
                for t, p in ingested["paths"].items()]

    @task
    def report(results) -> str:
        import pandas as pd
        rows = [row for per_flight in results for row in per_flight]
        out = PROJECT / "qc_report.csv"
        pd.DataFrame(rows).to_csv(out, index=False)
        return str(out)

    flights = list_flights()
    ingested = ingest.expand(ulg_path=flights)
    checked = validate.expand(ingested=ingested)
    report(checked)

flight_pipeline()