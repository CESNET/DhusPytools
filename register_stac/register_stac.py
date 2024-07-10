#!/usr/bin/python3

import argparse
import netrc
import os
import re
import sys
import tempfile
from datetime import datetime
from urllib.parse import urlparse

import defusedxml.ElementTree
import requests
import stactools.sentinel1.grd.stac
import stactools.sentinel2.stac
import stactools.sentinel3.stac
import stactools.sentinel5p.stac
import yaml
from tqdm import tqdm

import sentinel_stac


def parse_arguments():
    """
    Parse command line arguments. Check if combinations are valid.
    """
    parser = argparse.ArgumentParser(
        description='Generate Sentinel 1, 2, 3 and 5P metadata in STAC format from data fetched from a Sentinel OData '
                    'API. Configuration needs to be stored in register_stac.yml file and can be partly overwritten '
                    'by arguments. See README. The program requires --productId (-i) argument to be '
                    'provided and --save (-s) and/or --push (-p) option to be used. Example usage: '
                    './register_stac.py -p -i 72250006-4290-40ec-987d-3ed771e690f3')
    parser.add_argument('-i',
                        '--productId',
                        required=True,
                        help='UUID of product to generate STAC data for.')
    parser.add_argument('-e',
                        '--sentinelHost',
                        required=False,
                        help='URL of server to fetch Sentinel data from, for example https://dhr1.cesnet.cz/.'
                             'Overwrites SENTINEL_HOST configuration option.')
    parser.add_argument('-t',
                        '--stacHost',
                        required=False,
                        help='URL of server to push data to, for example https://stac.cesnet.cz.'
                             'Overwrites STAC_HOST configuration option.')
    parser.add_argument('-l',
                        '--stacLocalDir',
                        required=False,
                        help='Local folder to which STAC json files shall be stored if --save option specified. '
                             'Overwrites STAC_LOCAL_DIR configuration option.')
    parser.add_argument('-p',
                        '--push',
                        required=False,
                        action='store_true',
                        help='Enables pushing data to the catalogue at --stacHost.')
    parser.add_argument('-s',
                        '--save',
                        action='store_true',
                        help='Enables saving data locally.')

    args = parser.parse_args()
    if args.push and not args.stacHost:
        raise Exception('--push requires --stacHost argument')
    if not args.push and not args.save:
        raise Exception('--push or --save required to take any action')
    return args


def read_configuration():
    """
    Read configuration file.
    """
    with open(sentinel_stac.config_file, "r") as f:
        return yaml.safe_load(f)


def create_missing_dir(dir_path):
    """
    Creates directory, if it does not exist yet (including all missing directories in the path).
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)


def request_with_progress(url, output_path):
    """
    Downloads a file from a URL and saves it to the specified output path, with a progress bar.
    """
    # if ~/.netrc file is found, it is used automatically as a basic auth for all requests
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))  # Total size in bytes
    block_size = 1024  # Size of each block (1 KB)

    if not response.ok:
        raise Exception(f"Request to fetch file: {url} failed with {response.status_code}.\n{response.text}")

    progress_bar = tqdm(total=total_size,
                        unit='iB',
                        unit_scale=True,
                        desc=f"Fetching file {output_path.split('/')[-1]}",
                        leave=True,
                        file=sys.stdout)

    with open(output_path, "wb") as f:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            f.write(data)

    progress_bar.close()


def fetch_product_data(sentinel_host, product_id, metadata_dir):
    """
    Fetch Sentinel data for given product UUID from the specified host.
    """
    url = f"{sentinel_host}/odata/v1/Products('{product_id}')/Nodes"
    output_path = os.path.join(metadata_dir, "node.xml")
    request_with_progress(url, output_path)
    with open(os.path.join(output_path), "rb") as f:
        metadata = f.read()
        data = defusedxml.ElementTree.fromstring(metadata)
    namespaces = {'atom': 'http://www.w3.org/2005/Atom'}

    entry_node = data.find('atom:entry', namespaces)
    title_node = entry_node.find('atom:title', namespaces) if entry_node is not None else None
    title = title_node.text if title_node is not None else None
    product_node = entry_node.find('atom:id', namespaces) if entry_node is not None else None
    product_url = product_node.text if product_node is not None else None
    platform = title[0:2] if title else None
    collection = map_to_collection(title)

    if not title or not product_url:
        raise Exception(f"Missing required attributes for product {product_id}.")

    print(f"Fetched product data for product (UUID {product_id}):\n"
          f"*  Title ID: {title}\n"
          f"*  Platform: {platform}\n"
          f"*  Collection: {collection}\n"
          f"*  Product URL: {product_url}")
    return title, product_url, platform, collection


def check_hosts(sentinel_host, stac_host, push):
    """
    Checks sentinel_host and stac_host variables were resolved and .netrc file contains authentication credentials.
    """
    if not sentinel_host:
        raise Exception("Sentinel host not configured properly!")
    if not stac_host and push:
        raise Exception("STAC host not configured properly!")

    try:
        auth_info = netrc.netrc()
        if not auth_info.authenticators(urlparse(sentinel_host).netloc):
            raise Exception(
                f"Host {urlparse(sentinel_host)} not found in authentication credentials in the .netrc file!")
        if push and not auth_info.authenticators(urlparse(stac_host).netloc):
            raise Exception(f"Host {urlparse(stac_host)} not found in authentication credentials in the .netrc file!")
    except (FileNotFoundError, netrc.NetrcParseError) as e:
        raise Exception(f"Error parsing authentication file .netrc in the home directory: {e}")


def map_to_collection(product_name):
    """
    Returns the normalized collection name for a given product.
    """
    for pattern, collection in sentinel_stac.product_collection_mapping.items():
        if re.match(pattern, product_name):
            return collection
    raise Exception("Could not match product to collection name! Probably missing in the sentinel_stac.py mappings.")


def fetch_platform_metadata(product_url, metadata_dir, platform):
    """
    Fetches metadata from product's /Nodes data and stores them in the metadata directory.
    """
    if platform.lower() == "s1":
        platform_files = sentinel_stac.s1_files
    elif platform.lower() == "s2":
        platform_files = sentinel_stac.s2_files
    elif platform.lower() == "s3":
        platform_files = sentinel_stac.s3_files
    elif platform.lower() == "s5":
        platform_files = sentinel_stac.s5_files
    else:
        raise Exception(f"Platform {platform} not supported!")
    for file in platform_files:
        source_url = f"{product_url}/Nodes('{file}')/$value"
        output_file = os.path.join(metadata_dir, file)
        request_with_progress(source_url, output_file)


def fetch_nested_s1_files(safe_manifest, product_url, metadata_dir):
    """
    From the processed manifest file downloads the missing metadata files, which we know
    the stactools will be working with.
    """
    filepaths = safe_manifest.annotation_hrefs + safe_manifest.noise_hrefs + safe_manifest.calibration_hrefs
    for ref_name, filepath in filepaths:
        url_path_extension = filepath.split(f"{metadata_dir}/")[1]
        url_path_segments = url_path_extension.split("/")
        nested_file_url = product_url + ''.join(f"/Nodes('{segment}')" for segment in url_path_segments) + "/$value"
        create_missing_dir(os.path.dirname(filepath))
        request_with_progress(nested_file_url, filepath)


def fetch_nested_s2_files(safe_manifest, product_url, metadata_dir):
    """
    From the processed manifest file downloads the missing metadata files, which we know
    the stactools will be working with.
    """
    filepaths = [safe_manifest.product_metadata_href,
                 safe_manifest.granule_metadata_href,
                 safe_manifest.inspire_metadata_href,
                 safe_manifest.datastrip_metadata_href,
                 ]
    for filepath in filepaths:
        url_path_extension = filepath.split(f"{metadata_dir}/")[1]
        url_path_segments = url_path_extension.split("/")
        nested_file_url = product_url + ''.join(f"/Nodes('{segment}')" for segment in url_path_segments) + "/$value"
        create_missing_dir(os.path.dirname(filepath))
        request_with_progress(nested_file_url, filepath)


def fetch_s5_metadata(product_url, title, metadata_dir):
    """
    Fetches metadata directly from the product node - {{hostname}}/odata/v1/Products('{{UUID}}')/Node({{'title'}}/$value
    and stores them in a file named by the product title in the metadata directory.
    """
    url = f"{product_url}/$value"
    output_file = os.path.join(metadata_dir, title)
    request_with_progress(url, output_file)


def regenerate_href_links(stacfile_path, metadata_dir, product_url):
    """
    Replaces href links in the final json containing the local path to contain the OData path and format.
    Cannot use stactools' create_item function parameters for this change, as the stac module is then actually reading
    from the hrefs, or the change does not affect all the hrefs.
    """
    print("Regenerating href links")
    new_file = stacfile_path.split("/")
    new_file[-1] = 'new_' + new_file[-1]
    new_file = os.path.join('/', *new_file)
    with (open(stacfile_path, 'r') as infile, open(new_file, 'w') as outfile):
        for line in infile:
            if metadata_dir in line:
                split_line = line.split('"')  # [' ', 'href', ': ', 'fullpath', '\n']
                url_path_segments = split_line[-2].split(f"{metadata_dir}/")[1].split("/")
                correct_link = product_url + ''.join(
                    f"/Nodes('{segment}')" for segment in url_path_segments) + "/$value"
                split_line[-2] = correct_link
                outfile.write('"'.join(split_line))
            else:
                outfile.write(line)
    os.replace(new_file, stacfile_path)


def upload_to_catalogue(stac_host, collection, platform, stac_filepath, product_id, err_prefix, succ_prefix):
    """
    Uploads the stac file to the catalogue.
    Reports progress in the preconfigured files suffixed by the current date.
    """
    url = f"{stac_host}/collections/${collection[{platform}]}/items"
    print(f"Uploading STAC data to {url}")
    response = requests.post(stac_host, files={'file': open(stac_filepath, 'rb')})
    rundate = datetime.now().strftime('%Y-%m-%d')

    if response.ok:
        succ_file = succ_prefix + rundate
        create_missing_dir(os.path.dirname(succ_file))
        with open(succ_file, 'a') as f:
            f.write(f"{collection},{product_id}\n")
    else:
        err_file = err_prefix + rundate
        create_missing_dir(os.path.dirname(err_file))
        with open(err_file, 'a') as f:
            f.write(f"{collection},{product_id},{response.status_code}\n")
        if response.status_code == 404:
            raise Exception(f"Collection {collection} apparently does not exist on the server!")
        elif response.status_code == 409:
            raise Exception(f"Product {product_id} already registered on the server!")
        else:
            raise Exception(f"Request to upload STAC file failed with {response.status_code}.\n{response.text}")


def main():
    args = parse_arguments()
    config = read_configuration()
    product_id = args.productId

    sentinel_host = args.sentinelHost or config.get("SENTINEL_HOST")
    stac_host = args.stacHost or config.get("STAC_HOST")
    check_hosts(sentinel_host, stac_host, args.push)
    if args.save and config.get("STAC_LOCAL_DIR") is None and args.stacLocalDir is None:
        raise Exception("Flag --save was provided, but STAC_LOCAL_DIR option not configured and not specified "
                        "in the --stacLocalDir argument!")
    stac_storage = args.stacLocalDir or config.get("STAC_LOCAL_DIR")
    if stac_storage is not None:
        create_missing_dir(os.path.dirname(stac_storage))
    succ_prefix = config.get("SUCC_PREFIX")
    err_prefix = config.get("ERR_PREFIX")
    if args.push and (succ_prefix is None or err_prefix is None):
        raise Exception("Flag --push was provided, but SUCC_PREFIX and ERR_PREFIX need to be set in the configuration "
                        "file!")

    with (tempfile.TemporaryDirectory() as metadata_dir):
        print(f"Created temporary directory: {metadata_dir}")

        title, product_url, platform, collection = fetch_product_data(sentinel_host, product_id, metadata_dir)

        # create product archive folder
        metadata_dir = os.path.join(metadata_dir, title)
        os.mkdir(metadata_dir)

        fetch_platform_metadata(product_url, metadata_dir, platform)

        if platform.lower() == "s1":
            safe_manifest = stactools.sentinel1.grd.stac.MetadataLinks(metadata_dir)
            fetch_nested_s1_files(safe_manifest, product_url, metadata_dir)
            item = stactools.sentinel1.grd.stac.create_item(granule_href=metadata_dir)
        elif platform.lower() == "s2":
            safe_manifest = stactools.sentinel2.stac.SafeManifest(metadata_dir)
            fetch_nested_s2_files(safe_manifest, product_url, metadata_dir)
            item = stactools.sentinel2.stac.create_item(granule_href=metadata_dir)
        elif platform.lower() == "s3":
            item = stactools.sentinel3.stac.create_item(granule_href=metadata_dir)
        elif platform.lower() == "s5":
            fetch_s5_metadata(product_url, title, metadata_dir)
            item = stactools.sentinel5p.stac.create_item(os.path.join(metadata_dir, title))
        else:
            raise Exception(f"Unknown platform {platform}!")

        stac_storage = stac_storage if stac_storage is not None else metadata_dir
        stac_filepath = os.path.join(stac_storage, "{}.json".format(item.id))

        print(f"Writing metadata to file: {stac_filepath}")
        item.save_object(dest_href=stac_filepath, include_self_link=False)

        regenerate_href_links(stac_filepath, metadata_dir, product_url)

        if args.push:
            upload_to_catalogue(stac_host, collection, platform, stac_filepath, product_id, err_prefix, succ_prefix)

        print("Finished")
        exit()


if __name__ == "__main__":
    main()
