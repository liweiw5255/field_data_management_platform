#!/usr/bin/env python3
"""
Download SP/AE/WS data for a given date range.

Usage:
    python download_data.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD --source sp
    python download_data.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD --source ae
    python download_data.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD --source ws
    python download_data.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
"""

import argparse
import os
import sys
from datetime import datetime, timedelta


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
sys.path.insert(0, ROOT_DIR)

from config import AE_USERNAME, AE_PASSWORD, SP_USERNAME, SP_PASSWORD, WS_API_KEY
from sp.SunnyPortal import SunnyPortal
from ae.AlsoEnergy import AlsoEnergy
from ws.WeatherStation import WeatherStation


def parse_args():
    parser = argparse.ArgumentParser(description="Download SP/AE/WS data.")
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--source",
        choices=["sp", "ae", "ws"],
        default=None,
        help="Data source to download (sp, ae, ws). If omitted, downloads all.",
    )
    return parser.parse_args()


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def download_sp(start_date: datetime, end_date: datetime) -> None:
    sp_path = os.path.join(ROOT_DIR, "sp", "sp_data/")
    op_dir = os.path.join(sp_path, "operating")
    env_dir = os.path.join(sp_path, "environmental")
    os.makedirs(op_dir, exist_ok=True)
    os.makedirs(env_dir, exist_ok=True)
    driver_path = os.path.join(ROOT_DIR, "chromedriver_palmetto", "chromedriver")
    chrome_path = os.path.join(ROOT_DIR, "chrome_palmetto", "chrome")

    current_date = start_date
    all_exist = True
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        op_path = os.path.join(op_dir, f"sp_{date_str}.csv")
        env_path = os.path.join(env_dir, f"sp_{date_str}.csv")
        if not (os.path.isfile(op_path) and os.path.isfile(env_path)):
            all_exist = False
            break
        current_date = current_date + timedelta(days=1)

    if all_exist:
        print("SP data already exists for the full date range; skipping download.")
        return

    sp_object = SunnyPortal(sp_path, chrome_path, driver_path)
    sp_object.setUserName(SP_USERNAME)
    sp_object.setPassword(SP_PASSWORD)
    sp_object.setStartDate(start_date)
    sp_object.setEndDate(end_date)
    sp_object.SunnyPortal()


def download_ae(start_date: datetime, end_date: datetime) -> None:
    ae_path = os.path.join(ROOT_DIR, "ae", "ae_data/")
    driver_path = os.path.join(ROOT_DIR, "chromedriver_palmetto", "chromedriver")
    chrome_path = os.path.join(ROOT_DIR, "chrome_palmetto", "chrome")

    ae_object = AlsoEnergy(ae_path, driver_path, chrome_path)
    ae_object.setUserName(AE_USERNAME)
    ae_object.setPassword(AE_PASSWORD)
    ae_object.setStartDate(start_date)
    ae_object.setEndDate(end_date)
    ae_object.AlsoEnergy()


def download_ws(start_date: datetime, end_date: datetime) -> None:
    ws_path = os.path.join(ROOT_DIR, "ws", "ws_data/")
    ws_object = WeatherStation(ws_path, WS_API_KEY)
    ws_object.setStartDate(start_date)
    ws_object.setEndDate(end_date)
    ws_object.WeatherStation()


def main():
    args = parse_args()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    if start_date > end_date:
        raise ValueError("start-date must be before or equal to end-date")

    if args.source == "sp":
        download_sp(start_date, end_date)
    elif args.source == "ae":
        download_ae(start_date, end_date)
    elif args.source == "ws":
        download_ws(start_date, end_date)
    else:
        download_sp(start_date, end_date)
        download_ae(start_date, end_date)
        download_ws(start_date, end_date)


if __name__ == "__main__":
    main()
