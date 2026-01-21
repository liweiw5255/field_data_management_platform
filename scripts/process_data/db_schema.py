#!/usr/bin/env python3
import os
import re
import sqlite3


def get_root_dir():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def get_default_db_path():
    root_dir = get_root_dir()
    data_dir = os.path.join(root_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "field_data.sqlite")


RAW_SP_COLUMNS = [
    "time",
    "ac_power",
    "ac_power_l1",
    "ac_power_l2",
    "ac_power_l3",
    "ac_reactive_power",
    "ac_reactive_power_l1",
    "ac_reactive_power_l2",
    "ac_reactive_power_l3",
    "ac_apparent_power",
    "ac_apparent_power_l1",
    "ac_apparent_power_l2",
    "ac_apparent_power_l3",
    "ac_voltage_l1",
    "ac_voltage_l2",
    "ac_voltage_l3",
    "ac_current_l1",
    "ac_current_l2",
    "ac_current_l3",
    "grid_frequency",
    "dc_power_a",
    "dc_power_b",
    "dc_voltage_a",
    "dc_voltage_b",
    "dc_current_a",
    "dc_current_b",
    "iso",
    "ir",
    "ambient_temp",
    "ambient_rh",
    "cap_temp",
    "relay_temp",
    "rh",
    "inverterNo",
]

RAW_AE_COLUMNS = [
    "time",
    "ghi",
    "poa",
    "ambient_temp",
    "module_temp",
]

RAW_WS_COLUMNS = [
    "time",
    "ambient_temperature",
    "relative_humidity",
    "weather_condition",
]

PROCESSED_SP_COLUMNS = RAW_SP_COLUMNS[:]
PROCESSED_AE_COLUMNS = RAW_AE_COLUMNS[:]
PROCESSED_WS_COLUMNS = RAW_WS_COLUMNS[:] + ["weather_score"]

DE_RAW_HEADERS = [
    "Time",
    "Current NG (RMS) (RMS)",
    "Current NG (RMS)",
    "Current AN (RMS) (RMS)",
    "Current AN (RMS)",
    "Current LineToNeutralAverage (RMS)",
    "Current BN (RMS)",
    "Current CN (RMS)",
    "Current AN (TotalTHD)",
    "Current BN (TotalTHD)",
    "Current CN (TotalTHD)",
    "Power Total (S)",
    "Power Total (Q)",
    "Power Total (P)",
    "Voltage AN (TotalTHD)",
    "Voltage BN (TotalTHD)",
    "Voltage CN (TotalTHD)",
    "Voltage AN (FlkrPLT)",
    "Voltage BN (FlkrPLT)",
    "Voltage BN (FlkrPST)",
    "Voltage CN (FlkrPLT)",
    "Voltage CN (FlkrPST)",
    "Voltage AN (FlkrPST)",
    "Voltage AN (RMS)",
    "Voltage BN (RMS)",
    "Voltage CN (RMS)",
    "Voltage LineToNeutralAverage (RMS)",
]


def normalize_de_header(header: str) -> str:
    normalized = header.strip()
    if normalized.lower() == "time":
        return "time"
    normalized = normalized.replace("LineToNeutralAverage", "Line_To_Neutral_Average")
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized.lower()


DE_HEADER_TO_COLUMN = {header: normalize_de_header(header) for header in DE_RAW_HEADERS}
DE_COLUMNS = [DE_HEADER_TO_COLUMN[header] for header in DE_RAW_HEADERS]
PROCESSED_DE_COLUMNS = DE_COLUMNS[:]


def ensure_raw_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sp (
            time TEXT NOT NULL,
            ac_power REAL,
            ac_power_l1 REAL,
            ac_power_l2 REAL,
            ac_power_l3 REAL,
            ac_reactive_power REAL,
            ac_reactive_power_l1 REAL,
            ac_reactive_power_l2 REAL,
            ac_reactive_power_l3 REAL,
            ac_apparent_power REAL,
            ac_apparent_power_l1 REAL,
            ac_apparent_power_l2 REAL,
            ac_apparent_power_l3 REAL,
            ac_voltage_l1 REAL,
            ac_voltage_l2 REAL,
            ac_voltage_l3 REAL,
            ac_current_l1 REAL,
            ac_current_l2 REAL,
            ac_current_l3 REAL,
            grid_frequency REAL,
            dc_power_a REAL,
            dc_power_b REAL,
            dc_voltage_a REAL,
            dc_voltage_b REAL,
            dc_current_a REAL,
            dc_current_b REAL,
            iso REAL,
            ir REAL,
            ambient_temp REAL,
            ambient_rh REAL,
            cap_temp REAL,
            relay_temp REAL,
            rh REAL,
            inverterNo TEXT NOT NULL,
            PRIMARY KEY (time, inverterNo)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ae (
            time TEXT PRIMARY KEY,
            ghi REAL,
            poa REAL,
            ambient_temp REAL,
            module_temp REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ws (
            time TEXT PRIMARY KEY,
            ambient_temperature REAL,
            relative_humidity REAL,
            weather_condition TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inverter_list (
            inverterNo TEXT PRIMARY KEY,
            deviceID TEXT
        )
        """
    )
    conn.commit()


def ensure_processed_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_sp (
            time TEXT NOT NULL,
            ac_power REAL,
            ac_power_l1 REAL,
            ac_power_l2 REAL,
            ac_power_l3 REAL,
            ac_reactive_power REAL,
            ac_reactive_power_l1 REAL,
            ac_reactive_power_l2 REAL,
            ac_reactive_power_l3 REAL,
            ac_apparent_power REAL,
            ac_apparent_power_l1 REAL,
            ac_apparent_power_l2 REAL,
            ac_apparent_power_l3 REAL,
            ac_voltage_l1 REAL,
            ac_voltage_l2 REAL,
            ac_voltage_l3 REAL,
            ac_current_l1 REAL,
            ac_current_l2 REAL,
            ac_current_l3 REAL,
            grid_frequency REAL,
            dc_power_a REAL,
            dc_power_b REAL,
            dc_voltage_a REAL,
            dc_voltage_b REAL,
            dc_current_a REAL,
            dc_current_b REAL,
            iso REAL,
            ir REAL,
            ambient_temp REAL,
            ambient_rh REAL,
            cap_temp REAL,
            relay_temp REAL,
            rh REAL,
            inverterNo TEXT NOT NULL,
            PRIMARY KEY (time, inverterNo)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_ae (
            time TEXT PRIMARY KEY,
            ghi REAL,
            poa REAL,
            ambient_temp REAL,
            module_temp REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_ws (
            time TEXT PRIMARY KEY,
            ambient_temperature REAL,
            relative_humidity REAL,
            weather_condition TEXT,
            weather_score REAL
        )
        """
    )
    de_column_defs = ["time TEXT PRIMARY KEY"]
    for column in DE_COLUMNS[1:]:
        de_column_defs.append(f"{column} REAL")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS de (
            {", ".join(de_column_defs)}
        )
        """
    )
    conn.commit()


def upsert_inverter_list(conn: sqlite3.Connection) -> None:
    try:
        from sp.SunnyPortal import SunnyPortal
    except Exception:
        return

    inverter_list = SunnyPortal.inverterList
    rows = [(inv_no, device_id) for device_id, inv_no in inverter_list.items()]
    conn.executemany(
        "INSERT OR REPLACE INTO inverter_list (inverterNo, deviceID) VALUES (?, ?)",
        rows,
    )
    conn.commit()
