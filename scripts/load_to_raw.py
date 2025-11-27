from pathlib import Path

import pandas as pd
from sqlalchemy import text

from scripts.db_utils import get_engine


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

FILES = {
    "raw_accounts": DATA_DIR / "ravenstack_accounts.csv",
    "raw_subscriptions": DATA_DIR / "ravenstack_subscriptions.csv",
    "raw_feature_usage": DATA_DIR / "ravenstack_feature_usage.csv",
    "raw_support_tickets": DATA_DIR / "ravenstack_support_tickets.csv",
    "raw_churn_events": DATA_DIR / "ravenstack_churn_events.csv",
}


def truncate_raw():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                TRUNCATE TABLE
                    raw.raw_churn_events,
                    raw.raw_support_tickets,
                    raw.raw_feature_usage,
                    raw.raw_subscriptions,
                    raw.raw_accounts
                RESTART IDENTITY CASCADE;
                """
            )
        )
    print("RAW tables truncated.")


def load_table(table_name: str, csv_path: Path):
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    print(f"Loading {csv_path.name} into raw.{table_name} ...")
    df = pd.read_csv(csv_path)

    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(
            name=table_name,
            schema="raw",
            con=conn,
            if_exists="append",
            index=False,
        )
    print(f"Loaded {len(df)} rows into raw.{table_name}")


def main():
    truncate_raw()
    for table, path in FILES.items():
        load_table(table, path)
    print("All RavenStack CSVs loaded.")


if __name__ == "__main__":
    main()