import os

import pytest
from sqlalchemy import create_engine

from datapipe.store.database import DBConn


@pytest.fixture
def dbconn():
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    db_conn_str = f"postgresql://postgres:password@{pg_host}:{pg_port}/postgres"
    db_test_schema = "test"

    eng = create_engine(db_conn_str)

    try:
        eng.execute(f"DROP SCHEMA {db_test_schema} CASCADE")
    except (Exception,) as _:
        pass

    eng.execute(f"CREATE SCHEMA {db_test_schema}")

    yield DBConn(db_conn_str, db_test_schema)

    eng.execute(f"DROP SCHEMA {db_test_schema} CASCADE")
