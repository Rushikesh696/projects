"""
Integration tests — PostgreSQL connectivity and schema validation.
Requires: docker-compose up -d postgres
"""

import os
from pathlib import Path

import psycopg2
import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def db_conn():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    yield conn
    conn.close()


class TestDatabaseConnectivity:

    def test_connection_succeeds(self, db_conn):
        assert db_conn.closed == 0

    def test_database_name(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]
        assert db_name == os.getenv("POSTGRES_DB")


class TestSchemaExists:

    @pytest.mark.parametrize("schema", ["source", "bronze", "silver", "gold", "public"])
    def test_schema_exists(self, db_conn, schema):
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
                (schema,),
            )
            result = cur.fetchone()
        assert result is not None, f"Schema '{schema}' does not exist"


class TestTableExists:

    @pytest.mark.parametrize(
        "schema,table",
        [
            ("source", "qa_events"),
            ("bronze", "qa_events"),
            ("silver", "qa_events_clean"),
            ("gold", "qa_features"),
            ("public", "alerts"),
        ],
    )
    def test_table_exists(self, db_conn, schema, table):
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            )
            result = cur.fetchone()
        assert result is not None, f"Table '{schema}.{table}' does not exist"


class TestRowCounts:

    def test_bronze_has_rows(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.qa_events")
            count = cur.fetchone()[0]
        assert count > 0, "bronze.qa_events is empty"

    def test_silver_has_rows(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver.qa_events_clean")
            count = cur.fetchone()[0]
        assert count > 0, "silver.qa_events_clean is empty"

    def test_gold_has_rows(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gold.qa_features")
            count = cur.fetchone()[0]
        assert count > 0, "gold.qa_features is empty"

    def test_silver_fewer_rows_than_bronze(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.qa_events")
            bronze = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM silver.qa_events_clean")
            silver = cur.fetchone()[0]
        assert silver < bronze, "Silver should have fewer rows than Bronze (dedup applied)"

    def test_gold_matches_silver_row_count(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver.qa_events_clean")
            silver = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gold.qa_features")
            gold = cur.fetchone()[0]
        assert gold == silver, f"Gold ({gold}) should match Silver ({silver}) row count"
