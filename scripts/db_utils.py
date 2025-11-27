from pathlib import Path

from sqlalchemy import create_engine, text

DB_USER = "saas_user"
DB_PASS = "saas_password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "saas_warehouse"


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, future=True)


def run_sql(sql: str):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql))


def run_sql_file(path: str):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    sql = p.read_text()
    run_sql(sql)
