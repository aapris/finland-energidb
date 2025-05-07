import argparse
import logging
import os
from typing import Tuple

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException


def get_influxdb_args() -> Tuple[str, str, str, str]:
    """
    Parse InfluxDB connection parameters from command line arguments or get them from envs.

    :param env: True, if get arguments from envs.
    :return: url, token, org, bucket
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--influxdb-url", help="InfluxDB url", default=os.getenv("INFLUXDB_URL"), required=False)
    parser.add_argument("--influxdb-token", help="InxluxDB token", default=os.getenv("INFLUXDB_TOKEN"), required=False)
    parser.add_argument(
        "--influxdb-org", help="InfluxDB organization", default=os.getenv("INFLUXDB_ORG"), required=False
    )
    parser.add_argument(
        "--influxdb-bucket", help="InfluxDB bucket name", default=os.getenv("INFLUXDB_BUCKET"), required=False
    )
    args, unknown = parser.parse_known_args()
    args_dict = vars(args)
    # Check that mandatory variables are present
    for arg in ["INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_ORG", "INFLUXDB_BUCKET"]:
        if args_dict[arg.lower()] is None:
            raise RuntimeError(
                "Parameter missing: add --{} or {} environment variable".format(arg.lower().replace("_", "-"), arg)
            )
    url, token, org, bucket = args.influxdb_url, args.influxdb_token, args.influxdb_org, args.influxdb_bucket
    logging.info(f"Got InfluxDB parameters url={url}, token=*****, org={org}, bucket={bucket}")
    return url, token, org, bucket


def create_influxdb_client(url: str, token: str, org: str) -> InfluxDBClient:
    """
    Initialize InfluxDBClient using authentication token and InfluxDB url.

    :return: InfluxDBClient
    """
    # You can generate a Token from the "Tokens Tab" in the UI
    return InfluxDBClient(url=url, token=token, org=org)


def write_dataframe_to_influxdb(df, measurement_name, tag_columns=None, field_columns=None, output_filename=None):
    """
    Write pandas DataFrame to InfluxDB.

    Args:
        df: pandas DataFrame to write
        measurement_name: InfluxDB measurement name
        tag_columns: List of column names to use as tags
        field_columns: List of column names to use as fields
        output_filename: Optional filename(s) to check if output exists when handling errors

    Returns:
        bool: True if write was successful, False otherwise
    """
    try:
        url, token, org, bucket = get_influxdb_args()
        client = create_influxdb_client(url, token, org)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        logging.info(f"Write DataFrame into {url}/{org}/{bucket}")
        logging.debug(df)

        write_api.write(
            bucket=bucket,
            record=df,
            data_frame_measurement_name=measurement_name,
            data_frame_tag_columns=tag_columns,
            data_frame_field_columns=field_columns,
        )
        return True
    except ApiException as e:
        if e.status == 401:
            logging.critical(f"Authentication error with InfluxDB: {e.reason}. Check your token and permissions.")
        else:
            logging.critical(f"InfluxDB API error: {e.status} - {e.reason}")
        return False
    except (ValueError, TypeError, RuntimeError) as e:
        logging.error(f"Error writing DataFrame to InfluxDB: {e}")
        return False
