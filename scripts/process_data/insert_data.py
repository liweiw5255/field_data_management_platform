#!/usr/bin/env python3
import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, ROOT_DIR)

from db_schema import (
    RAW_AE_COLUMNS,
    RAW_SP_COLUMNS,
    RAW_WS_COLUMNS,
    ensure_raw_tables,
    get_default_db_path,
    upsert_inverter_list,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Insert raw data into SQLite.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--source",
        choices=["sp", "ae", "ws"],
        default=None,
        help="Source to insert. If omitted, inserts all.",
    )
    parser.add_argument(
        "--db-path",
        default=get_default_db_path(),
        help="SQLite database path (default: data/field_data.sqlite).",
    )
    parser.add_argument(
        "--exception-value",
        type=float,
        default=-1.0,
        help="Value used to fill missing numeric data.",
    )
    return parser.parse_args()


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def ensure_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def fill_missing_values(df: pd.DataFrame, exception_value: float) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if col == "time":
            continue
        if df[col].dtype == object:
            df[col] = df[col].fillna("UNKNOWN")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].fillna(exception_value)
    return df


def load_ae_data(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    ae_path = os.path.join(ROOT_DIR, "ae", "ae_data")
    dfs = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        filename = os.path.join(ae_path, f"ae_{date_str}.csv")
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            if df.columns[0].startswith("Unnamed") or "index" in df.columns[0].lower():
                df = df.drop(columns=[df.columns[0]])
            dfs.append(df)
        current += timedelta(days=1)

    if not dfs:
        return pd.DataFrame(columns=RAW_AE_COLUMNS)

    df = pd.concat(dfs, ignore_index=True)
    if "Time" in df.columns:
        df = df.rename(columns={"Time": "time"})
    elif "time" in df.columns:
        pass
    else:
        df = df.rename(columns={df.columns[0]: "time"})

    rename_map = {
        "GHI": "ghi",
        "POA": "poa",
        "ambient_temp": "ambient_temp",
        "module_temp": "module_temp",
    }
    df = df.rename(columns=rename_map)
    df = df[["time", "ghi", "poa", "ambient_temp", "module_temp"]]
    df = ensure_datetime(df, "time")
    df = df.sort_values("time").reset_index(drop=True)
    return df


def load_ws_data(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    ws_path = os.path.join(ROOT_DIR, "ws", "ws_data")
    dfs = []
    current = start_date - timedelta(days=1)
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        filename = os.path.join(ws_path, f"ws_{date_str}.csv")
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            dfs.append(df)
        current += timedelta(days=1)

    if not dfs:
        return pd.DataFrame(columns=RAW_WS_COLUMNS)

    df = pd.concat(dfs, ignore_index=True)
    df = ensure_datetime(df, "time")
    df = df.sort_values("time").reset_index(drop=True)
    return df


def build_sp_dataframe(sp_op_df: pd.DataFrame, sp_env_df: pd.DataFrame) -> pd.DataFrame:
    sp_op_df = sp_op_df.reset_index(drop=True)
    sp_env_df = sp_env_df.reset_index(drop=True)
    sp_df_origin = pd.concat([sp_op_df, sp_env_df], axis=1)

    if sp_df_origin.shape[1] < 36:
        raise ValueError("SP data does not have enough columns to build combined dataframe")

    sp_df = pd.concat(
        [
            sp_df_origin.iloc[:, :27],
            sp_df_origin.iloc[:, 32:33],
            sp_df_origin.iloc[:, 30:32],
            sp_df_origin.iloc[:, 33:36],
            sp_df_origin.iloc[:, 27:28],
        ],
        axis=1,
    )
    sp_df.columns = RAW_SP_COLUMNS
    return sp_df


def load_sp_data(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    sp_path = os.path.join(ROOT_DIR, "sp", "sp_data")
    op_dfs = []
    env_dfs = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        op_filename = os.path.join(sp_path, "operating", f"sp_{date_str}.csv")
        env_filename = os.path.join(sp_path, "environmental", f"sp_{date_str}.csv")
        if os.path.exists(op_filename) and os.path.exists(env_filename):
            op_dfs.append(pd.read_csv(op_filename))
            env_dfs.append(pd.read_csv(env_filename))
        current += timedelta(days=1)

    if not op_dfs or not env_dfs:
        return pd.DataFrame(columns=RAW_SP_COLUMNS)

    sp_op_df = pd.concat(op_dfs, ignore_index=True)
    sp_env_df = pd.concat(env_dfs, ignore_index=True)
    sp_df = build_sp_dataframe(sp_op_df, sp_env_df)
    sp_df = ensure_datetime(sp_df, "time")
    sp_df = sp_df.sort_values(["time", "inverterNo"]).reset_index(drop=True)
    return sp_df


def fill_missing_ae(df: pd.DataFrame, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    if df.empty:
        return df
    df = ensure_datetime(df, "time")
    df = df.dropna(subset=["time"])
    if df["time"].duplicated().any():
        df = df.groupby("time", as_index=False).mean(numeric_only=True)
    start_ts = start_date.replace(hour=0, minute=0, second=0)
    end_ts = end_date.replace(hour=23, minute=59, second=0)
    full_range = pd.date_range(start=start_ts, end=end_ts, freq="1min")
    df = df.set_index("time").reindex(full_range).reset_index()
    df = df.rename(columns={"index": "time"})
    return df


def fill_missing_ws(df: pd.DataFrame, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    if df.empty:
        return df
    df = ensure_datetime(df, "time")
    df = df.dropna(subset=["time"])
    df["time"] = df["time"].dt.round("60min")
    numeric_cols = ["ambient_temperature", "relative_humidity"]
    df_numeric = df.groupby("time", as_index=False)[numeric_cols].mean()
    df_weather = (
        df.groupby("time", as_index=False)["weather_condition"]
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else "UNKNOWN")
    )
    df = pd.merge(df_numeric, df_weather, on="time", how="outer")

    start_ts = start_date.replace(hour=0, minute=0, second=0)
    end_ts = end_date.replace(hour=23, minute=0, second=0)
    full_range = pd.date_range(start=start_ts, end=end_ts, freq="60min")
    df = df.set_index("time").reindex(full_range).reset_index()
    df = df.rename(columns={"index": "time"})
    return df


def insert_dataframe(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    placeholders = ",".join(["?"] * len(df.columns))
    columns = ",".join(df.columns)
    sql = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
    conn.executemany(sql, df.itertuples(index=False, name=None))
    conn.commit()


def main():
    args = parse_args()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if start_date > end_date:
        raise ValueError("start-date must be before or equal to end-date")

    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    with sqlite3.connect(args.db_path) as conn:
        ensure_raw_tables(conn)
        upsert_inverter_list(conn)

        if args.source in (None, "ae"):
            ae_df = load_ae_data(start_date, end_date)
            ae_df = fill_missing_ae(ae_df, start_date, end_date)
            ae_df = fill_missing_values(ae_df, args.exception_value)
            ae_df["time"] = ae_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            insert_dataframe(conn, "ae", ae_df[RAW_AE_COLUMNS])

        if args.source in (None, "ws"):
            ws_df = load_ws_data(start_date, end_date)
            ws_df = fill_missing_ws(ws_df, start_date, end_date)
            ws_df = fill_missing_values(ws_df, args.exception_value)
            ws_df["time"] = ws_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            insert_dataframe(conn, "ws", ws_df[RAW_WS_COLUMNS])

        if args.source in (None, "sp"):
            sp_df = load_sp_data(start_date, end_date)
            if not sp_df.empty:
                sp_df = fill_missing_values(sp_df, args.exception_value)
                sp_df["time"] = sp_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
                insert_dataframe(conn, "sp", sp_df[RAW_SP_COLUMNS])


if __name__ == "__main__":
    main()
