from sqlalchemy import text
from scripts.db_utils import get_engine


def main():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT * FROM mart.mart_mrr_over_time ORDER BY month LIMIT 10;")
        )
        for row in result:
            print(row)


if __name__ == "__main__":
    main()