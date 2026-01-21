#!/usr/bin/env python3
import argparse
import os
import sqlite3
import sys


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from db_schema import ensure_raw_tables, get_default_db_path, upsert_inverter_list


def parse_args():
    parser = argparse.ArgumentParser(description="Create raw SQLite tables.")
    parser.add_argument(
        "--db-path",
        default=get_default_db_path(),
        help="SQLite database path (default: data/field_data.sqlite).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    with sqlite3.connect(args.db_path) as conn:
        ensure_raw_tables(conn)
        upsert_inverter_list(conn)
    print(f"Raw tables created in {args.db_path}")


if __name__ == "__main__":
    main()
