#!/usr/bin/python3

import argparse
import os
from datetime import datetime, timedelta

import requests
import yaml

DEBUG = False
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
CONFIG_FILE = "sentinel_config.yml"
LIST_FILENAME = "gen_new_list_processed.txt"
TIMESTAMP_FILENAME = "gen_new_list_timestamp.txt"


def parse_arguments():
    """
    Parse command line arguments. Check if combinations are valid.
    """
    parser = argparse.ArgumentParser(
        description='Generates a list of Sentinel products recently published at DHuS endpoint.'
                    'Example usage: ./gen_new_list.py')
    parser.add_argument('-r',
                        '--dryRun',
                        required=False,
                        action='store_true',
                        help='Do not store results')
    parser.add_argument('-f',
                        "--fromTimestamp",
                        required=False,
                        type=lambda d: datetime.strptime(d, '%Y-%m-%d'),
                        help="Alternative start date to use instead of stored timestamp.")
    parser.add_argument('-e',
                        '--sentinelHost',
                        required=False,
                        help='URL of server to fetch Sentinel data from, for example https://dhr1.cesnet.cz/.'
                             'Overwrites SENTINEL_HOST configuration option.')
    parser.add_argument('-d',
                        '--debug',
                        required=False,
                        action="store_true",
                        help='Enable to see enable extended progress messages.')

    args = parser.parse_args()
    return args


def print_debug(msg):
    """
    Prints debug message to console if DEBUG variable is True.
    """
    if DEBUG:
        print(msg)


def read_configuration():
    """
    Read configuration file.
    """
    with open(CONFIG_FILE, "r") as f:
        return yaml.safe_load(f)


def get_timestamp(local_dir):
    """
    Reads timestamp of last script run. If file does not exist or is malformed, fallbacks to last 31 days.
    Timestamp needs to be trimmed to max. 3 millisecond decimal places.
    """
    timestamp_filepath = os.path.join(local_dir, TIMESTAMP_FILENAME)
    fallback_timestamp = (datetime.now() - timedelta(days=31)).strftime(DATE_FORMAT)[:-3]
    if not os.path.isfile(timestamp_filepath) or not os.path.getsize(timestamp_filepath):
        return fallback_timestamp
    with open(timestamp_filepath, "r") as f:
        content = f.read().strip()
        try:
            timestamp = content[:-3]
            print_debug(f"Using stored timestamp {timestamp}")
            return timestamp
        except ValueError:
            print("Timestamp file exists but is formatted incorrectly")
    return fallback_timestamp


def create_missing_dir(dir_path):
    """
    Creates directory, if it does not exist yet (including all missing directories in the path).
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)


def fetch_products(host_url, timestamp):
    """
    Fetches all products created after given timestamp
    """
    fetched_count = 100
    page_size = 100
    result = []

    # header is included in response
    while fetched_count + 1 >= page_size:
        url = f"{host_url}/odata/v1/Products"
        params = {
            '$format': 'text/csv',
            '$select': 'Id',
            '$skip': len(result),
            '$top': page_size,
            '$filter': f"CreationDate ge datetime'{timestamp}'"
        }
        response = requests.get(url, params=params)

        if not response.ok:
            raise Exception(f"Request to fetch products file failed with {response.status_code}.\n{response.text}")

        product_ids = response.text.splitlines()[1:]
        result.extend(product_ids)
        fetched_count = len(product_ids)
    print_debug(f"Fetched {len(result)} products.")
    return result


def load_cached_products(local_dir):
    """
    Loads file containing last processed product ids, if the file exists.
    """
    filepath = os.path.join(local_dir, LIST_FILENAME)
    if not os.path.exists(filepath):
        return []
    else:
        with open(filepath, "r") as f:
            return f.readlines()


def store_new_timestamp(local_dir, new_timestamp):
    """
    Overwrites last time of processing with new timestamp.
    """
    timestamp_filepath = os.path.join(local_dir, TIMESTAMP_FILENAME)
    with open(timestamp_filepath, 'w') as f:
        f.write(new_timestamp)


def store_new_list(local_dir, missing_products):
    """
    Overwrites last processed product ids with new ones.
    """
    list_filepath = os.path.join(local_dir, LIST_FILENAME)
    with open(list_filepath, 'w') as f:
        f.write("\n".join(missing_products))


def main():
    args = parse_arguments()
    config = read_configuration()

    global DEBUG
    DEBUG = args.debug
    sentinel_host = args.sentinelHost or config.get("SENTINEL_HOST")
    if not sentinel_host:
        raise Exception("SENTINEL_HOST is not defined and sentinelHost parameter not passed!")
    local_dir = config.get("LOCAL_DIR")

    timestamp = args.fromTimestamp or get_timestamp(local_dir)

    new_timestamp = datetime.now().strftime(DATE_FORMAT)
    fetched_products = fetch_products(sentinel_host, timestamp)
    stored_products = load_cached_products(local_dir)
    missing_products = list(set(fetched_products) - set(stored_products))
    print_debug(f"There are {len(missing_products)} unprocessed products.")

    if not args.dryRun:
        store_new_timestamp(local_dir, new_timestamp)
        store_new_list(local_dir, missing_products)


if __name__ == "__main__":
    main()
