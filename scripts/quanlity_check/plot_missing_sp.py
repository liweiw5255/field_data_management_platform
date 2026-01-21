#!/usr/bin/env python3
import argparse
import os
import sqlite3
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from zoneinfo import ZoneInfo


ROOT_DIR = "/home/admin/Desktop/field_data_management_platform"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot missing SP timestamp counts as a month/day heatmap."
    )
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--inverter-no",
        default="30",
        help="SP inverterNo to analyze (default: 30).",
    )
    parser.add_argument(
        "--db-path",
        default=os.path.join(ROOT_DIR, "data", "field_data.sqlite"),
        help="SQLite database path (for AE GHI mask).",
    )
    parser.add_argument(
        "--interval-min",
        type=int,
        default=5,
        help="Expected sampling interval in minutes (default: 5).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output path to save the figure base name.",
    )
    parser.add_argument(
        "--summary-csv",
        default=None,
        help="Optional path base to save per-date missing counts (CSV).",
    )
    return parser.parse_args()


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def load_times_from_file(path, time_col="time"):
    if not os.path.exists(path):
        return pd.Series(dtype="datetime64[ns]")
    df = pd.read_csv(path)
    if time_col not in df.columns:
        return pd.Series(dtype="datetime64[ns]")
    times = pd.to_datetime(df[time_col], errors="coerce")
    return times.dropna()


def get_operating_times(date, inverter_no):
    op_path = os.path.join(
        ROOT_DIR, "sp", "sp_data", "operating", f"sp_{date:%Y-%m-%d}.csv"
    )
    if not os.path.exists(op_path):
        return pd.Series(dtype="datetime64[ns]")
    df = pd.read_csv(op_path)
    if "time" not in df.columns:
        return pd.Series(dtype="datetime64[ns]")
    if "deviceID" in df.columns:
        df = df[df["deviceID"].astype(str) == str(inverter_no)]
    times = pd.to_datetime(df["time"], errors="coerce")
    return times.dropna()


def get_environmental_times(date):
    env_path = os.path.join(
        ROOT_DIR, "sp", "sp_data", "environmental", f"sp_{date:%Y-%m-%d}.csv"
    )
    return load_times_from_file(env_path, "time")


def expected_points_for_date(date, interval_min, tz):
    start = datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=tz)
    end = start + timedelta(days=1)
    offset_start = start.utcoffset()
    offset_end = end.utcoffset()
    if offset_start is None or offset_end is None:
        return int(24 * 60 / interval_min)
    delta_hours = (offset_end - offset_start).total_seconds() / 3600.0
    if delta_hours > 0:
        # Spring forward: 23-hour day
        return int(23 * 60 / interval_min)
    if delta_hours < 0:
        # Fall back: data files typically do not repeat the hour
        return int(24 * 60 / interval_min)
    return int(24 * 60 / interval_min)


def load_ae_ghi_mask(conn, date, interval_min):
    start_ts = date.strftime("%Y-%m-%d 00:00:00")
    end_ts = date.strftime("%Y-%m-%d 23:59:59")
    df = pd.read_sql_query(
        "SELECT time, ghi FROM ae WHERE time BETWEEN ? AND ?",
        conn,
        params=[start_ts, end_ts],
    )
    if df.empty:
        return set()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time", "ghi"])
    df = df[df["ghi"] > 0]
    times = df["time"].dt.floor(f"{interval_min}min").dt.strftime("%Y-%m-%d %H:%M:%S")
    return set(times.tolist())


def compute_negative_counts(data_loader, start_date, end_date, interval_min, ghi_masks):
    date_range = pd.date_range(start_date, end_date, freq="D")
    counts = []
    for date in date_range:
        mask_times = ghi_masks.get(date.date(), set())
        if not mask_times:
            counts.append({"date": date.date(), "neg_count": 0})
            continue
        day_df = data_loader(date.to_pydatetime())
        if day_df.empty:
            counts.append({"date": date.date(), "neg_count": len(mask_times)})
            continue
        if "time" not in day_df.columns:
            counts.append({"date": date.date(), "neg_count": len(mask_times)})
            continue
        day_df["time"] = pd.to_datetime(day_df["time"], errors="coerce")
        day_df = day_df.dropna(subset=["time"])
        day_df["time_key"] = day_df["time"].dt.floor(f"{interval_min}min").dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        day_df = day_df[day_df["time_key"].isin(mask_times)]
        if day_df.empty:
            counts.append({"date": date.date(), "neg_count": 0})
            continue
        data_cols = [
            col for col in day_df.columns if col not in ("time", "time_key", "deviceID")
        ]
        mask_neg = (day_df[data_cols] == -1).any(axis=1)
        if mask_neg.any():
            per_time = (
                day_df.loc[mask_neg, ["time_key"]]
                .groupby("time_key")
                .size()
            )
            neg_count = int((per_time > 0).sum())
        else:
            neg_count = 0
        counts.append({"date": date.date(), "neg_count": neg_count})
    return pd.DataFrame(counts)


def compute_field_missing_counts(data_loader, start_date, end_date, interval_min, ghi_masks):
    date_range = pd.date_range(start_date, end_date, freq="D")
    rows = []
    for date in date_range:
        mask_times = ghi_masks.get(date.date(), set())
        if not mask_times:
            continue
        day_df = data_loader(date.to_pydatetime())
        if day_df.empty or "time" not in day_df.columns:
            continue
        day_df["time"] = pd.to_datetime(day_df["time"], errors="coerce")
        day_df = day_df.dropna(subset=["time"])
        day_df["time_key"] = day_df["time"].dt.floor(f"{interval_min}min").dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        day_df = day_df[day_df["time_key"].isin(mask_times)]
        if day_df.empty:
            continue
        data_cols = [
            col for col in day_df.columns if col not in ("time", "time_key", "deviceID")
        ]
        grouped = {}
        for col in data_cols:
            if col.endswith(("_a", "_b")):
                base = col[:-2]
            elif col.endswith(("_l1", "_l2", "_l3")):
                base = col[:-3]
            else:
                base = col
            grouped.setdefault(base, []).append(col)

        for base, cols in grouped.items():
            missing_count = int((day_df[cols] == -1).any(axis=1).sum())
            rows.append(
                {
                    "date": date.date(),
                    "field": base,
                    "missing_count": missing_count,
                }
            )
    if not rows:
        return pd.DataFrame(columns=["date", "field", "missing_count"])
    return pd.DataFrame(rows)


def build_heatmap_matrix(df, value_col):
    df["month"] = pd.to_datetime(df["date"]).dt.month
    df["day"] = pd.to_datetime(df["date"]).dt.day

    matrix = np.full((12, 31), np.nan)
    for record in df.itertuples():
        matrix[record.month - 1, record.day - 1] = getattr(record, value_col)
    return matrix


def plot_heatmap(matrix, title, cbar_label, interval_min):
    fig, ax = plt.subplots(figsize=(12, 6))
    im = ax.imshow(matrix, aspect="auto", cmap="coolwarm")
    ax.set_title(
        title
    )
    ax.set_xlabel("Day")
    ax.set_ylabel("Month")
    ax.set_xticks(np.arange(31))
    ax.set_xticklabels(np.arange(1, 32))
    ax.set_yticks(np.arange(12))
    ax.set_yticklabels(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    )

    for i in range(12):
        for j in range(31):
            if not np.isnan(matrix[i, j]):
                ax.text(j, i, f"{int(matrix[i, j])}", ha="center", va="center", color="white")

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label(cbar_label)

    ax.text(
        0.0,
        -0.18,
        f"Counts are based on {interval_min}-minute intervals "
        f"(expected per day varies with DST).",
        transform=ax.transAxes,
    )
    fig.tight_layout()
    return fig


def plot_daily_totals(df, dataset, start_date, end_date):
    data = df[df["dataset"] == dataset].copy()
    if data.empty:
        return None
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values("date")
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(data["date"], data["missing_count"], linewidth=1.2)
    ax.set_title(
        f"Daily Missing (-1) Counts during Daytime (AE GHI > 0) - {dataset}"
    )
    ax.set_xlabel("Date")
    ax.set_ylabel("Missing Count")
    ax.set_xlim(pd.to_datetime(start_date), pd.to_datetime(end_date))
    fig.tight_layout()
    return fig


def plot_field_heatmap(df, dataset):
    data = df[df["dataset"] == dataset].copy()
    if data.empty:
        return None
    data["date"] = pd.to_datetime(data["date"])
    pivot = data.pivot_table(
        index="field", columns="date", values="missing_count", aggfunc="sum"
    ).fillna(0)
    fig, ax = plt.subplots(figsize=(12, 6))
    im = ax.imshow(pivot.values, aspect="auto", cmap="coolwarm")
    ax.set_title(f"Missing (-1) Counts by Field and Date - {dataset}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Field")
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_yticklabels(pivot.index)

    # Monthly ticks on x-axis
    dates = pivot.columns
    month_starts = [i for i, d in enumerate(dates) if d.day == 1]
    ax.set_xticks(month_starts)
    ax.set_xticklabels([dates[i].strftime("%b") for i in month_starts])

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Missing Count")
    fig.tight_layout()
    return fig


def main():
    args = parse_args()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if start_date > end_date:
        raise ValueError("start-date must be before or equal to end-date")

    tz = ZoneInfo("America/New_York")
    with sqlite3.connect(args.db_path) as conn:
        ghi_masks = {}
        for date in pd.date_range(start_date, end_date, freq="D"):
            ghi_masks[date.date()] = load_ae_ghi_mask(conn, date, args.interval_min)

    env_negative_df = compute_negative_counts(
        lambda date: pd.read_csv(
            os.path.join(
                ROOT_DIR,
                "sp",
                "sp_data",
                "environmental",
                f"sp_{date:%Y-%m-%d}.csv",
            )
        )
        if os.path.exists(
            os.path.join(
                ROOT_DIR,
                "sp",
                "sp_data",
                "environmental",
                f"sp_{date:%Y-%m-%d}.csv",
            )
        )
        else pd.DataFrame(),
        start_date,
        end_date,
        args.interval_min,
        ghi_masks,
    )
    env_matrix = build_heatmap_matrix(env_negative_df, "neg_count")
    fig = plot_heatmap(
        env_matrix,
        "Count of -1 Values during Daytime (AE GHI > 0) for SP Environmental Data "
        f"({start_date:%Y-%m-%d} to {end_date:%Y-%m-%d})",
        "Count of rows with -1 values (AE GHI > 0)",
        args.interval_min,
    )
    if args.output:
        base, ext = os.path.splitext(args.output)
        fig.savefig(f"{base}_env{ext or '.png'}", dpi=300, bbox_inches="tight")
    plt.show()

    env_field_df = compute_field_missing_counts(
        lambda date: pd.read_csv(
            os.path.join(
                ROOT_DIR,
                "sp",
                "sp_data",
                "environmental",
                f"sp_{date:%Y-%m-%d}.csv",
            )
        )
        if os.path.exists(
            os.path.join(
                ROOT_DIR,
                "sp",
                "sp_data",
                "environmental",
                f"sp_{date:%Y-%m-%d}.csv",
            )
        )
        else pd.DataFrame(),
        start_date,
        end_date,
        args.interval_min,
        ghi_masks,
    )

    def load_operating_df(date):
        op_path = os.path.join(
            ROOT_DIR, "sp", "sp_data", "operating", f"sp_{date:%Y-%m-%d}.csv"
        )
        if not os.path.exists(op_path):
            return pd.DataFrame()
        df = pd.read_csv(op_path)
        if "deviceID" in df.columns:
            df = df[df["deviceID"].astype(str) == str(args.inverter_no)]
        return df

    op_negative_df = compute_negative_counts(
        load_operating_df,
        start_date,
        end_date,
        args.interval_min,
        ghi_masks,
    )
    op_matrix = build_heatmap_matrix(op_negative_df, "neg_count")
    fig = plot_heatmap(
        op_matrix,
        "Count of -1 Values during Daytime (AE GHI > 0) for SP Operating Data "
        f"(Inverter {args.inverter_no}) ({start_date:%Y-%m-%d} to {end_date:%Y-%m-%d})",
        "Count of rows with -1 values (AE GHI > 0)",
        args.interval_min,
    )
    if args.output:
        base, ext = os.path.splitext(args.output)
        fig.savefig(f"{base}_op{ext or '.png'}", dpi=300, bbox_inches="tight")
    plt.show()

    op_field_df = compute_field_missing_counts(
        load_operating_df,
        start_date,
        end_date,
        args.interval_min,
        ghi_masks,
    )

    summary_df = pd.concat(
        [
            env_field_df.assign(dataset="environmental"),
            op_field_df.assign(dataset="operating"),
        ],
        ignore_index=True,
    )
    if not summary_df.empty:
        per_date_total = (
            summary_df.groupby(["dataset", "date"], as_index=False)["missing_count"]
            .sum()
            .sort_values(["dataset", "date"])
        )
        per_date_field = summary_df.sort_values(
            ["dataset", "date", "missing_count"], ascending=[True, True, False]
        )

        print("Missing totals per date (AE GHI > 0):")
        print(per_date_total.to_string(index=False))
        print("\nMissing counts per date and field (AE GHI > 0):")
        print(per_date_field.to_string(index=False))

        if args.summary_csv:
            base, ext = os.path.splitext(args.summary_csv)
            per_date_total.to_csv(f"{base}_daily{ext or '.csv'}", index=False)
            per_date_field.to_csv(f"{base}_daily_fields{ext or '.csv'}", index=False)

        for dataset in ["environmental", "operating"]:
            fig = plot_daily_totals(per_date_total, dataset, start_date, end_date)
            if fig is not None:
                if args.output:
                    base, ext = os.path.splitext(args.output)
                    fig.savefig(
                        f"{base}_{dataset}_daily{ext or '.png'}",
                        dpi=300,
                        bbox_inches="tight",
                    )
                plt.show()
            fig = plot_field_heatmap(per_date_field, dataset)
            if fig is not None:
                if args.output:
                    base, ext = os.path.splitext(args.output)
                    fig.savefig(
                        f"{base}_{dataset}_fields{ext or '.png'}",
                        dpi=300,
                        bbox_inches="tight",
                    )
                plt.show()


if __name__ == "__main__":
    main()
