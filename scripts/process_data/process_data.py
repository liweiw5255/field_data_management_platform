#!/usr/bin/env python3
import argparse
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime

import numpy as np
import pandas as pd


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, SCRIPT_DIR)

from db_schema import (
    DE_COLUMNS,
    DE_HEADER_TO_COLUMN,
    PROCESSED_AE_COLUMNS,
    PROCESSED_DE_COLUMNS,
    PROCESSED_SP_COLUMNS,
    PROCESSED_WS_COLUMNS,
    ensure_processed_tables,
    get_default_db_path,
)


SEVERE_WEATHER = ["Haze", "Thunder", "Storm", "Heavy", "Drizzle", "T-storm", "T-Storm"]
MILD_WEATHER = ["Cloudy", "Rain", "Fog", "Smoke", "Mist"]

DE_DATA_DIR = os.path.join(ROOT_DIR, "de", "de_data")
DE_CSV_DIR = os.path.join(ROOT_DIR, "de", "de_data_csv")
PQDIF_DIR = os.path.join(ROOT_DIR, "scripts", "Pqdif")
PQDIF_PROJECT = os.path.join(PQDIF_DIR, "Pqdif.csproj")
PQDIF_DOTNET = os.path.join(PQDIF_DIR, "dotnet")


def parse_args():
    parser = argparse.ArgumentParser(description="Process data and insert into processed tables.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--source",
        choices=["sp", "ae", "ws", "de"],
        default=None,
        help="Source to process. If omitted, processes all.",
    )
    parser.add_argument(
        "--db-path",
        default=get_default_db_path(),
        help="SQLite database path (default: data/field_data.sqlite).",
    )
    return parser.parse_args()


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def parse_de_date_from_filename(filename: str) -> datetime | None:
    if not filename.endswith((".pqd", ".csv")):
        return None
    base = os.path.basename(filename)
    if len(base) < 8 or not base[:8].isdigit():
        return None
    try:
        return datetime.strptime(base[:8], "%Y%m%d")
    except ValueError:
        return None


def convert_de_pqd_to_csv(pqd_path: str, csv_dir: str) -> str:
    os.makedirs(csv_dir, exist_ok=True)
    csv_name = os.path.basename(os.path.splitext(pqd_path)[0] + ".csv")
    output_csv = os.path.join(csv_dir, csv_name)

    dotnet_cmd = PQDIF_DOTNET if os.path.exists(PQDIF_DOTNET) else "dotnet"
    cmd = [dotnet_cmd, "run", "--project", PQDIF_PROJECT, pqd_path]
    subprocess.run(
        cmd,
        cwd=PQDIF_DIR,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

    generated_csv = os.path.splitext(pqd_path)[0] + ".csv"
    if not os.path.exists(generated_csv):
        raise FileNotFoundError(f"CSV not generated for {pqd_path}")

    shutil.copy2(generated_csv, output_csv)
    return output_csv


def load_de_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if "Time" not in df.columns:
        raise ValueError(f"Missing Time column in {csv_path}")
    df = df.rename(columns=DE_HEADER_TO_COLUMN)

    file_date = parse_de_date_from_filename(csv_path)
    if file_date is None:
        raise ValueError(f"Unable to parse date from {csv_path}")

    parsed_time = pd.to_datetime(df["time"], format="%H:%M.%f", errors="coerce")
    if parsed_time.isna().all():
        parsed_time = pd.to_datetime(df["time"], format="%H:%M:%S", errors="coerce")
    if parsed_time.isna().all():
        parsed_time = pd.to_datetime(df["time"], errors="coerce")

    df["time"] = parsed_time.dt.time
    df["time"] = df["time"].apply(
        lambda t: datetime.combine(file_date.date(), t) if pd.notna(t) else pd.NaT
    )

    full_times = pd.date_range(
        start=file_date.replace(hour=0, minute=0, second=0, microsecond=0),
        end=file_date.replace(hour=23, minute=45, second=0, microsecond=0),
        freq="15min",
    )

    df = df.set_index("time").reindex(full_times)
    df.index.name = "time"

    for column in DE_COLUMNS[1:]:
        if column not in df.columns:
            df[column] = -1

    df = df[DE_COLUMNS[1:]].fillna(-1)
    df.insert(0, "time", df.index)
    df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def load_tmy_limits() -> dict:
    tmy_path = os.path.join(ROOT_DIR, "tmy")
    dfs = []
    for filename in os.listdir(tmy_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(tmy_path, filename)
            try:
                df = pd.read_csv(file_path, delimiter=",", skiprows=2)
                dfs.append(df)
            except Exception:
                continue
    if not dfs:
        raise ValueError("No TMY data files were loaded for limit calculation.")
    tmy_df = pd.concat(dfs, ignore_index=True)
    return {
        "max_ghi": max(tmy_df["GHI"]),
        "min_ghi": min(tmy_df["GHI"]),
        "max_temp": max(tmy_df["Temperature"]),
        "min_temp": min(tmy_df["Temperature"]),
        "max_rh": max(tmy_df["Relative Humidity"]),
        "min_rh": min(tmy_df["Relative Humidity"]),
    }


def read_table(conn: sqlite3.Connection, table: str, start: datetime, end: datetime) -> pd.DataFrame:
    start_ts = start.strftime("%Y-%m-%d 00:00:00")
    end_ts = end.strftime("%Y-%m-%d 23:59:59")
    query = f"SELECT * FROM {table} WHERE time BETWEEN ? AND ?"
    df = pd.read_sql_query(query, conn, params=[start_ts, end_ts])
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df


def clean_sp_data(sp_df: pd.DataFrame, limits: dict) -> pd.DataFrame:
    if sp_df.empty:
        return sp_df
    sp_df = sp_df.copy()
    mask_op = sp_df["ac_power"] == -1
    sp_df.loc[mask_op, "ir"] = 0.0
    sp_df.loc[mask_op, "dc_power_a"] = 0.0
    sp_df.loc[mask_op, "dc_power_b"] = 0.0

    sp_df.loc[sp_df["ir"] > limits["max_ghi"], "ir"] = np.nan
    sp_df.loc[sp_df["ir"] < limits["min_ghi"], "ir"] = np.nan
    sp_df.loc[sp_df["ambient_temp"] > limits["max_temp"], "ambient_temp"] = np.nan
    sp_df.loc[sp_df["ambient_temp"] < limits["min_temp"], "ambient_temp"] = np.nan
    sp_df.loc[sp_df["ambient_rh"] > limits["max_rh"], "ambient_rh"] = np.nan
    sp_df.loc[sp_df["ambient_rh"] < limits["min_rh"], "ambient_rh"] = np.nan

    sp_df["ir"] = sp_df["ir"].ffill()
    sp_df["ambient_temp"] = sp_df["ambient_temp"].ffill()
    sp_df["ambient_rh"] = sp_df["ambient_rh"].ffill()
    return sp_df


def clean_ae_data(ae_df: pd.DataFrame, limits: dict) -> pd.DataFrame:
    if ae_df.empty:
        return ae_df
    ae_df = ae_df.copy()
    ae_df.loc[ae_df["ghi"] > limits["max_ghi"], "ghi"] = np.nan
    ae_df.loc[ae_df["ghi"] < limits["min_ghi"], "ghi"] = np.nan
    ae_df["ghi"] = ae_df["ghi"].ffill()
    return ae_df


def add_weather_score(ws_df: pd.DataFrame) -> pd.DataFrame:
    if ws_df.empty:
        return ws_df
    ws_df = ws_df.copy()
    ws_df["weather_condition"] = ws_df["weather_condition"].fillna("UNKNOWN")
    ws_df["weather_score"] = 0
    for i in range(ws_df.shape[0]):
        points = 0
        condition_str = str(ws_df.loc[i, "weather_condition"])
        for part in condition_str.split("/"):
            part = part.strip()
            if part in SEVERE_WEATHER:
                points += 10
            elif part in MILD_WEATHER:
                points += 5
        ws_df.loc[i, "weather_score"] = points
    return ws_df


def insert_dataframe(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    placeholders = ",".join(["?"] * len(df.columns))
    columns = ",".join(df.columns)
    sql = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
    conn.executemany(sql, df.itertuples(index=False, name=None))
    conn.commit()


def delete_existing(conn: sqlite3.Connection, table: str, start: datetime, end: datetime) -> None:
    start_ts = start.strftime("%Y-%m-%d 00:00:00")
    end_ts = end.strftime("%Y-%m-%d 23:59:59")
    conn.execute(f"DELETE FROM {table} WHERE time BETWEEN ? AND ?", (start_ts, end_ts))
    conn.commit()


def main():
    args = parse_args()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if start_date > end_date:
        raise ValueError("start-date must be before or equal to end-date")

    limits = load_tmy_limits()

    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    with sqlite3.connect(args.db_path) as conn:
        ensure_processed_tables(conn)

        if args.source in (None, "ae"):
            ae_df = read_table(conn, "ae", start_date, end_date)
            ae_df = clean_ae_data(ae_df, limits)
            ae_df["time"] = ae_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            delete_existing(conn, "processed_ae", start_date, end_date)
            insert_dataframe(conn, "processed_ae", ae_df[PROCESSED_AE_COLUMNS])

        if args.source in (None, "ws"):
            ws_df = read_table(conn, "ws", start_date, end_date)
            ws_df = add_weather_score(ws_df)
            ws_df["time"] = ws_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            delete_existing(conn, "processed_ws", start_date, end_date)
            insert_dataframe(conn, "processed_ws", ws_df[PROCESSED_WS_COLUMNS])

        if args.source in (None, "sp"):
            sp_df = read_table(conn, "sp", start_date, end_date)
            sp_df = clean_sp_data(sp_df, limits)
            sp_df["time"] = sp_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            delete_existing(conn, "processed_sp", start_date, end_date)
            insert_dataframe(conn, "processed_sp", sp_df[PROCESSED_SP_COLUMNS])

        if args.source in (None, "de"):
            os.makedirs(DE_CSV_DIR, exist_ok=True)
            delete_existing(conn, "de", start_date, end_date)

            for filename in sorted(os.listdir(DE_DATA_DIR)):
                file_date = parse_de_date_from_filename(filename)
                if file_date is None:
                    continue
                if not (start_date.date() <= file_date.date() <= end_date.date()):
                    continue
                pqd_path = os.path.join(DE_DATA_DIR, filename)
                if not pqd_path.endswith(".pqd"):
                    continue

                csv_path = convert_de_pqd_to_csv(pqd_path, DE_CSV_DIR)
                de_df = load_de_csv(csv_path)
                insert_dataframe(conn, "de", de_df[PROCESSED_DE_COLUMNS])


if __name__ == "__main__":
    main()
