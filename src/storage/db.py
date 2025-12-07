# src/storage/db.py
from __future__ import annotations

import os
import hashlib
from typing import Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# -------------------------
# Connection helper
# -------------------------

def _get_conn():
    """
    Connect to Postgres using env vars.
    Defaults are set for your Docker container on port 54321.
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "54321"))  # you mapped 54321:5432
    db   = os.getenv("POSTGRES_DB", "personal_finance_tracker_db")
    user = os.getenv("POSTGRES_USER", "user")        # change to 'postgres' if that's your role
    pwd  = os.getenv("POSTGRES_PASSWORD", "123")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pwd,
    )


# -------------------------
# Natural key helpers
# -------------------------

def _hash_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _make_payment_nk(row: pd.Series) -> str:
    """
    Natural key for payments:
    (source, statement_file, trans_date, post_date, description, amount)
    """
    parts = [
        str(row.get("source", "CIBC")),
        str(row.get("statement_file", "")),
        str(row.get("trans_date", "")),
        str(row.get("post_date", "")),
        str(row.get("description", "")),
        f"{row.get('amount', 0):.2f}",
    ]
    return _hash_str("|".join(parts))


def _make_txn_nk(row: pd.Series) -> str:
    """
    Natural key for transactions:
    (source, statement_file, trans_date, post_date, description, category, amount)
    """
    parts = [
        str(row.get("source", "CIBC")),
        str(row.get("statement_file", "")),
        str(row.get("trans_date", "")),
        str(row.get("post_date", "")),
        str(row.get("description", "")),
        str(row.get("category", "")),
        f"{row.get('amount', 0):.2f}",
    ]
    return _hash_str("|".join(parts))


# -------------------------
# DDL (schema/tables/indexes)
# -------------------------

DDL_SQL = """
CREATE SCHEMA IF NOT EXISTS expense;

CREATE TABLE IF NOT EXISTS expense.payments (
    id              BIGSERIAL PRIMARY KEY,
    natural_key     TEXT NOT NULL UNIQUE,
    trans_date      DATE NOT NULL,
    post_date       DATE NOT NULL,
    description     TEXT NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    source          TEXT NOT NULL DEFAULT 'CIBC',
    statement_file  TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS expense.transactions (
    id              BIGSERIAL PRIMARY KEY,
    natural_key     TEXT NOT NULL UNIQUE,
    trans_date      DATE NOT NULL,
    post_date       DATE NOT NULL,
    description     TEXT NOT NULL,
    category        TEXT NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    location        TEXT,
    city            TEXT,
    province        TEXT,
    source          TEXT NOT NULL DEFAULT 'CIBC',
    statement_file  TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_trans_date
    ON expense.transactions (trans_date);

CREATE INDEX IF NOT EXISTS idx_transactions_category
    ON expense.transactions (category);

CREATE INDEX IF NOT EXISTS idx_transactions_city
    ON expense.transactions (city);

CREATE INDEX IF NOT EXISTS idx_payments_trans_date
    ON expense.payments (trans_date);
"""


def init_db() -> None:
    """
    Initialize schema/tables/indexes in the current database.
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(DDL_SQL)
    finally:
        conn.close()


# -------------------------
# Upsert functions
# -------------------------

def upsert_payments(df: pd.DataFrame) -> Tuple[int, int]:
    """
    Insert into expense.payments with ON CONFLICT DO NOTHING.

    Returns: (inserted_count, skipped_count)
    """
    if df is None or df.empty:
        return 0, 0

    df = df.copy()

    # Ensure required columns exist
    if "statement_file" not in df.columns:
        df["statement_file"] = ""
    if "source" not in df.columns:
        df["source"] = "CIBC"

    # Convert datetime -> date (DB uses DATE)
    for col in ("trans_date", "post_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Natural keys
    df["natural_key"] = df.apply(_make_payment_nk, axis=1)

    rows = [
        (
            row["natural_key"],
            row["trans_date"],
            row["post_date"],
            row["description"],
            float(row["amount"]),
            row["source"],
            row["statement_file"],
        )
        for _, row in df.iterrows()
    ]

    sql = """
        INSERT INTO expense.payments (
            natural_key, trans_date, post_date,
            description, amount, source, statement_file
        )
        VALUES %s
        ON CONFLICT (natural_key) DO NOTHING;
    """

    conn = _get_conn()
    inserted = 0
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows)
                inserted = cur.rowcount if cur.rowcount is not None else 0
    finally:
        conn.close()

    skipped = len(df) - inserted
    return inserted, skipped


def upsert_transactions(df: pd.DataFrame) -> Tuple[int, int]:
    """
    Insert into expense.transactions with ON CONFLICT DO NOTHING.

    Returns: (inserted_count, skipped_count)
    """
    if df is None or df.empty:
        return 0, 0

    df = df.copy()

    if "statement_file" not in df.columns:
        df["statement_file"] = ""
    if "source" not in df.columns:
        df["source"] = "CIBC"

    for col in ("trans_date", "post_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    df["natural_key"] = df.apply(_make_txn_nk, axis=1)

    rows = [
        (
            row["natural_key"],
            row["trans_date"],
            row["post_date"],
            row["description"],
            row.get("category", "") or "",
            float(row["amount"]),
            row.get("location", "") or "",
            row.get("city", "") or "",
            row.get("province", "") or "",
            row["source"],
            row["statement_file"],
        )
        for _, row in df.iterrows()
    ]

    sql = """
        INSERT INTO expense.transactions (
            natural_key, trans_date, post_date,
            description, category, amount,
            location, city, province,
            source, statement_file
        )
        VALUES %s
        ON CONFLICT (natural_key) DO NOTHING;
    """

    conn = _get_conn()
    inserted = 0
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows)
                inserted = cur.rowcount if cur.rowcount is not None else 0
    finally:
        conn.close()

    skipped = len(df) - inserted
    return inserted, skipped
