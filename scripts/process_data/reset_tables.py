#!/usr/bin/env python3
import argparse
import os
import sqlite3

from db_schema import (
    ensure_processed_tables,
    ensure_raw_tables,
    get_default_db_path,
)


RAW_TABLES = ["sp", "ae", "ws", "inverter_list"]
PROCESSED_TABLES = ["processed_sp", "processed_ae", "processed_ws"]


def parse_args():
    parser = argparse.ArgumentParser(description="Reset SQLite tables.")
    parser.add_argument(
        "--db-path",
        default=get_default_db_path(),
        help="SQLite database path (default: data/field_data.sqlite).",
    )
    parser.add_argument(
        "--scope",
        choices=["raw", "processed", "all"],
        default="all",
        help="Which tables to reset.",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop tables instead of deleting rows.",
    )
    return parser.parse_args()


def reset_tables(conn: sqlite3.Connection, tables, drop: bool) -> None:
    for table in tables:
        if drop:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        else:
            conn.execute(f"DELETE FROM {table}")
    conn.commit()


def main():
    args = parse_args()
    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    with sqlite3.connect(args.db_path) as conn:
        if args.scope in ("raw", "all"):
            reset_tables(conn, RAW_TABLES, args.drop)
            if args.drop:
                ensure_raw_tables(conn)
        if args.scope in ("processed", "all"):
            reset_tables(conn, PROCESSED_TABLES, args.drop)
            if args.drop:
                ensure_processed_tables(conn)
    print(f"Reset completed for scope={args.scope} (drop={args.drop}).")


if __name__ == "__main__":
    main()
