from scripts.db_utils import run_sql_file


def main():
    run_sql_file("sql/staging/create_staging_tables.sql")
    run_sql_file("sql/mart/build_mrr_over_time.sql")
    print("Staging and mart rebuilt.")


if __name__ == "__main__":
    main()