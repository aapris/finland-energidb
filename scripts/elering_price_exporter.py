import argparse
import logging
import os
from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd
from utils.influxdb import write_dataframe_to_influxdb


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch electricity market prices")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--start-time", type=str, help="Start time in ISO format (e.g., 2025-05-07T22:00:00Z)")
    group.add_argument("--tomorrow", action="store_true", help="Fetch prices for tomorrow")
    group.add_argument(
        "--today", action="store_true", help="Fetch prices for today (from yesterday 22:00 to today 22:00)"
    )
    parser.add_argument("--end-time", type=str, help="End time in ISO format (e.g., 2025-05-08T23:00:00Z)")
    parser.add_argument("--influxdb-measurement", type=str, default="price", help="InfluxDB measurement name")
    parser.add_argument(
        "--output-filename", type=str, nargs="+", help="Output filenames (supported formats: .parquet, .csv, .xlsx)"
    )
    parser.add_argument("--log", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="ERROR")

    args, unknown = parser.parse_known_args()

    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))

    now = datetime.now(timezone.utc)

    if args.tomorrow:
        args.start_time = now.replace(hour=22, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
        args.end_time = (now.replace(hour=22, minute=0, second=0, microsecond=0) + timedelta(days=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    elif args.today:
        args.start_time = (now.replace(hour=22, minute=0, second=0, microsecond=0) - timedelta(days=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        args.end_time = now.replace(hour=22, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif not args.start_time:
        args.start_time = (now.replace(hour=22, minute=0, second=0, microsecond=0) - timedelta(days=3)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    if not args.end_time and not args.today and not args.tomorrow:
        args.end_time = (now.replace(hour=22, minute=0, second=0, microsecond=0) + timedelta(days=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    # Validate start and end time
    try:
        datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
        datetime.fromisoformat(args.end_time.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("Invalid date format. Please use ISO format (e.g., 2025-05-07T22:00:00Z)")

    return args


def fetch_electricity_prices(start_time, end_time):
    url = f"https://dashboard.elering.ee/api/nps/price?start={start_time}&end={end_time}"
    with httpx.Client() as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


def convert_to_dataframe(data):
    df_dict = {}
    for country, prices in data["data"].items():
        df_dict[country] = pd.DataFrame(prices).set_index("timestamp")["price"]

    df = pd.DataFrame(df_dict)
    df.index = pd.to_datetime(df.index, unit="s", utc=True)
    df.index.name = "time"

    # Convert wide format to long format (long format)
    df = df.reset_index().melt(id_vars=["time"], var_name="area", value_name="value")
    # Set time back to index
    df = df.set_index("time")

    return df


def save_to_file(df, filename):
    """Save DataFrame to a file based on the file extension."""
    ext = os.path.splitext(filename)[1].lower()

    if ext == ".parquet":
        df.reset_index().to_parquet(filename)
        logging.info(f"Data saved to Parquet file: {filename}")
    elif ext == ".csv":
        df.reset_index().to_csv(filename, index=False)
        logging.info(f"Data saved to CSV file: {filename}")
    elif ext == ".xlsx":
        df.reset_index().to_excel(filename, index=False)
        logging.info(f"Data saved to Excel file: {filename}")
    else:
        logging.warning(f"Unsupported file extension: {ext}. Skipping file: {filename}")


def main():
    args = parse_args()
    data = fetch_electricity_prices(args.start_time, args.end_time)
    # Convert country codes to uppercase
    data["data"] = {k.upper(): v for k, v in data["data"].items()}

    if data["success"]:
        df = convert_to_dataframe(data)
        logging.info(df)

        # Save to files if filenames are provided
        if args.output_filename:
            for filename in args.output_filename:
                save_to_file(df, filename)

        # Try to save to InfluxDB
        write_dataframe_to_influxdb(
            df,
            args.influxdb_measurement,
            tag_columns=["area"],
            field_columns=["value"],
        )
    else:
        logging.error("Failed to fetch data")


if __name__ == "__main__":
    main()
