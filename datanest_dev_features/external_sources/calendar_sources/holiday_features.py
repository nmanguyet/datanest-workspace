import pandas as pd
import numpy as np
from datetime import timedelta

HOLIDAY_CSV = None
OUTPUT_PATH = None
TET_KEYWORDS = ["lunar new year"]


def load_holidays(csv_path=None):
    path = csv_path or HOLIDAY_CSV
    pdf = pd.read_csv(path)
    pdf = pdf[pdf["scope"] == "national"].copy()
    pdf["date"] = pd.to_datetime(pdf["date"])
    pdf["end_date"] = pd.to_datetime(pdf["end_date"])
    pdf["is_tet"] = pdf["holiday"].str.lower().str.contains("|".join(TET_KEYWORDS))

    rows = []
    for _, r in pdf.iterrows():
        for d in pd.date_range(r["date"], r["end_date"]):
            rows.append({"date": d, "holiday": r["holiday"],
                         "is_national_holiday": 1, "is_tet": int(r["is_tet"])})

    result = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    result["year"] = result["date"].dt.year
    result["month"] = result["date"].dt.month
    result["week"] = result["date"].dt.isocalendar().week.astype(int)
    return result


def generate_holiday_features(holidays, start_date, end_date):
    dates = pd.date_range(start_date, end_date, freq="D")
    cal = pd.DataFrame({"date": dates})
    cal["year"] = cal["date"].dt.year
    cal["month"] = cal["date"].dt.month
    cal["week"] = cal["date"].dt.isocalendar().week.astype(int)
    cal["dow"] = cal["date"].dt.dayofweek

    holiday_agg = holidays.groupby("date", as_index=False).agg(
        is_national_holiday=("is_national_holiday", "max"),
        is_tet=("is_tet", "max")
    )
    cal = cal.merge(holiday_agg, on="date", how="left").fillna({"is_national_holiday": 0, "is_tet": 0})

    cal["is_national_holiday"] = cal["is_national_holiday"].astype(int)
    cal["is_tet"] = cal["is_tet"].astype(int)

    hdays = holidays[holidays["is_national_holiday"] == 1]["date"].drop_duplicates().sort_values()
    tdays = holidays[holidays["is_tet"] == 1]["date"].drop_duplicates().sort_values()

    h_arr = hdays.dt.date.values.astype("datetime64[ns]")
    t_arr = tdays.dt.date.values.astype("datetime64[ns]")

    def nearest_forward(dates_arr, target):
        target = np.datetime64(target.date())
        idx = dates_arr.searchsorted(target)
        if idx < len(dates_arr):
            return (dates_arr[idx] - target).astype("timedelta64[D]").astype(int)
        return 0

    def nearest_backward(dates_arr, target):
        target = np.datetime64(target.date())
        idx = dates_arr.searchsorted(target, side="right") - 1
        if idx >= 0:
            return (target - dates_arr[idx]).astype("timedelta64[D]").astype(int)
        return 0

    cal["days_to_next_national_holiday"] = cal["date"].apply(lambda d: nearest_forward(h_arr, d))
    cal["days_since_last_national_holiday"] = cal["date"].apply(lambda d: nearest_backward(h_arr, d))
    cal["days_to_tet"] = cal["date"].apply(lambda d: nearest_forward(t_arr, d))
    cal["days_since_tet"] = cal["date"].apply(lambda d: nearest_backward(t_arr, d))

    holiday_dates = set(cal[cal["is_national_holiday"] == 1]["date"].dt.date)
    nat_holidays = holidays[holidays["is_national_holiday"] == 1][["date", "holiday"]].drop_duplicates()

    def count_day_in_future(target, days_ahead):
        end = target.date() + timedelta(days=days_ahead)
        target_d = target.date()
        return sum(1 for d in holiday_dates if target_d < d <= end)

    def count_day_in_past(target, days_back):
        start = target.date() - timedelta(days=days_back)
        target_d = target.date()
        return sum(1 for d in holiday_dates if start <= d < target_d)

    def count_event_in_future(target, days_ahead):
        target_d = target.date()
        end = target_d + timedelta(days=days_ahead)
        mask = nat_holidays["date"].apply(lambda d: target_d < d.date() <= end)
        return nat_holidays.loc[mask, "holiday"].nunique()

    def count_event_in_past(target, days_back):
        target_d = target.date()
        start = target_d - timedelta(days=days_back)
        mask = nat_holidays["date"].apply(lambda d: start <= d.date() < target_d)
        return nat_holidays.loc[mask, "holiday"].nunique()

    cal["holiday_day_next_7d"] = cal["date"].apply(lambda d: count_day_in_future(d, 7))
    cal["holiday_day_last_7d"] = cal["date"].apply(lambda d: count_day_in_past(d, 7))
    cal["holiday_event_next_7d"] = cal["date"].apply(lambda d: count_event_in_future(d, 7))
    cal["holiday_event_last_7d"] = cal["date"].apply(lambda d: count_event_in_past(d, 7))

    month_counts = cal[cal["is_national_holiday"] == 1].groupby(["year", "month"]).size().reset_index(name="holidays_this_month")
    cal = cal.merge(month_counts, on=["year", "month"], how="left")
    cal["holidays_this_month"] = cal["holidays_this_month"].fillna(0).astype(int)

    tet_counts = cal[cal["is_tet"] == 1].groupby(["year", "month"]).size().reset_index(name="tet_this_month")
    cal = cal.merge(tet_counts, on=["year", "month"], how="left")
    cal["tet_this_month"] = cal["tet_this_month"].fillna(0).astype(int)

    week_counts = cal[cal["is_national_holiday"] == 1].groupby(["year", "week"]).size().reset_index(name="holidays_this_week")
    cal = cal.merge(week_counts, on=["year", "week"], how="left")
    cal["holidays_this_week"] = cal["holidays_this_week"].fillna(0).astype(int)

    cal["is_holiday_week"] = (cal["holidays_this_week"] > 0).astype(int)

    cal["is_weekend"] = (cal["dow"] >= 5).astype(int)

    return cal


def run(csv_path=None, output_path=None, start_date="2020-01-01", end_date="2026-12-31", weekly=True):
    path = csv_path or HOLIDAY_CSV
    out = output_path or OUTPUT_PATH
    holidays = load_holidays(path)
    result = generate_holiday_features(holidays, start_date, end_date)

    if weekly:
        result = result[result["dow"] == 6].reset_index(drop=True)

    cols = ["date", "days_to_tet", "days_since_tet",
            "holiday_day_next_7d", "holiday_day_last_7d",
            "holiday_event_next_7d", "holiday_event_last_7d"]
    result = result[cols]

    if out:
        result.to_csv(out, index=False)
        print(f"Saved {len(result)} rows to {out}")

    freq = "weekly (Sunday)" if weekly else "daily"
    print(f"Holiday features: {len(result)} {freq} snapshots, {len(cols)} columns")
    print(f"  Range: {result['date'].min().date()} -> {result['date'].max().date()}")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate Vietnam holiday calendar features")
    parser.add_argument("csv", help="Path to vietnam_holidays CSV")
    parser.add_argument("-o", "--output", help="Output CSV path (default: print summary only)")
    parser.add_argument("--start", default="2020-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-12-31", help="End date (YYYY-MM-DD)")
    parser.add_argument("--daily", action="store_true", help="Output daily instead of weekly Sunday snapshots")
    args = parser.parse_args()

    run(csv_path=args.csv, output_path=args.output,
        start_date=args.start, end_date=args.end,
        weekly=not args.daily)
