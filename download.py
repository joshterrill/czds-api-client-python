import json
import sys
import os
import datetime
import argparse
import glob as glob_module
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.message import Message
from rich.progress import (
    Progress,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
)
from rich.console import Console
from do_authentication import authenticate
from do_http_get import do_get, do_head

console = Console()

parser = argparse.ArgumentParser(description="Download CZDS zone files")
parser.add_argument(
    "--threads", type=int, default=1, help="Number of concurrent downloads (default: 1)"
)
parser.add_argument(
    "--check-etag",
    action="store_true",
    help="Check ETag before downloading; saves files with ETag in filename and skips if already exists",
)
args = parser.parse_args()

##############################################################################################################
# First Step: Get the config data from config.json file
##############################################################################################################

try:
    if "CZDS_CONFIG" in os.environ:
        config_data = os.environ["CZDS_CONFIG"]
        config = json.loads(config_data)
    else:
        with open("config.json", "r") as config_file:
            config = json.load(config_file)
except Exception as e:
    sys.stderr.write("Error loading config.json file: %s\n" % str(e))
    exit(1)

username = config["icann.account.username"]
password = config["icann.account.password"]
authen_base_url = config["authentication.base.url"]
czds_base_url = config["czds.base.url"]
tlds = config.get("tlds", [])
excluded_tlds = config.get("excluded_tlds", [])
working_directory = config.get("working.directory", ".")

if not username:
    sys.stderr.write(
        "'icann.account.username' parameter not found in the config.json file\n"
    )
    exit(1)

if not password:
    sys.stderr.write(
        "'icann.account.password' parameter not found in the config.json file\n"
    )
    exit(1)

if not authen_base_url:
    sys.stderr.write(
        "'authentication.base.url' parameter not found in the config.json file\n"
    )
    exit(1)

if not czds_base_url:
    sys.stderr.write("'czds.base.url' parameter not found in the config.json file\n")
    exit(1)

##############################################################################################################
# Second Step: authenticate the user to get an access_token.
##############################################################################################################

console.print(f"Authenticate user {username}")
access_token = authenticate(username, password, authen_base_url)

##############################################################################################################
# Third Step: Get the download zone file links
##############################################################################################################


def get_zone_links(czds_base_url):
    global access_token

    links_url = czds_base_url + "/czds/downloads/links"
    links_response = do_get(links_url, access_token)

    status_code = links_response.status_code

    if status_code == 200:
        zone_links = links_response.json()
        console.print(
            f"{datetime.datetime.now()}: The number of zone files to be downloaded is {len(tlds) or len(zone_links)}"
        )
        return zone_links
    elif status_code == 401:
        console.print(
            f"The access_token has been expired. Re-authenticate user {username}"
        )
        access_token = authenticate(username, password, authen_base_url)
        return get_zone_links(czds_base_url)
    else:
        sys.stderr.write(
            f"Failed to get zone links from {links_url} with error code {status_code}\n"
        )
        return None


zone_links = get_zone_links(czds_base_url)
if not zone_links:
    exit(1)

##############################################################################################################
# Fourth Step: download zone files
##############################################################################################################


def _parse_header(header):
    m = Message()
    m["content-type"] = header
    return m


def _clean_etag(etag):
    if not etag:
        return None
    return etag.strip('"').replace("/", "_").replace("\\", "_")


def _get_tld_from_url(url):
    return url.rsplit("/", 1)[-1].rsplit(".")[-2]


def _find_existing_file(output_directory, tld, etag, expected_size=None):
    pattern = os.path.join(output_directory, f"{tld}_*.txt.gz")
    existing_files = glob_module.glob(pattern)
    for filepath in existing_files:
        filename = os.path.basename(filepath)
        if f"_{etag}." in filename:
            if expected_size is not None:
                actual_size = os.path.getsize(filepath)
                if actual_size == expected_size:
                    return filepath
                # File exists but is incomplete/wrong size
                return None
            return filepath
    return None


def download_one_zone(url, output_directory, check_etag=False, progress=None):
    global access_token
    tld = _get_tld_from_url(url)
    etag = None

    # If check_etag is enabled, do a HEAD request first to get ETag
    if check_etag:
        head_response = do_head(url, access_token)
        if head_response.status_code == 200:
            etag = _clean_etag(head_response.headers.get("ETag"))
            expected_size = int(head_response.headers.get("Content-Length", 0)) or None
            if etag:
                existing_file = _find_existing_file(
                    output_directory, tld, etag, expected_size
                )
                if existing_file:
                    console.print(
                        f"{datetime.datetime.now()}: Skipping {tld} - already have complete file with ETag {etag}"
                    )
                    return True
        elif head_response.status_code == 401:
            console.print(
                f"The access_token has expired. Re-authenticate user {username}"
            )
            access_token = authenticate(username, password, authen_base_url)
            return download_one_zone(url, output_directory, check_etag, progress)

    download_zone_response = do_get(url, access_token)
    status_code = download_zone_response.status_code

    if status_code == 200:
        if not etag:
            etag = _clean_etag(download_zone_response.headers.get("ETag"))

        option = _parse_header(download_zone_response.headers["content-disposition"])
        filename = option.get_param("filename")

        if not filename:
            filename = tld + ".txt.gz"

        if check_etag and etag:
            name_part = filename.rsplit(".txt.gz", 1)[0]
            filename = f"{name_part}_{etag}.txt.gz"

        path = f"{output_directory}/{filename}"
        total_size = int(download_zone_response.headers.get("content-length", 0))
        chunk_size = 8192

        if progress:
            task_id = progress.add_task(filename, total=total_size)
            with open(path, "wb") as f:
                for chunk in download_zone_response.iter_content(chunk_size):
                    f.write(chunk)
                    progress.update(task_id, advance=len(chunk))
            progress.remove_task(task_id)
        else:
            with Progress(
                "[progress.description]{task.description}",
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.1f}%",
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
            ) as single_progress:
                task_id = single_progress.add_task(filename, total=total_size)
                with open(path, "wb") as f:
                    for chunk in download_zone_response.iter_content(chunk_size):
                        f.write(chunk)
                        single_progress.update(task_id, advance=len(chunk))

        console.print(
            f"{datetime.datetime.now()}: Completed downloading zone to file {path}"
        )
        return True

    elif status_code == 401:
        console.print(
            f"The access_token has been expired. Re-authenticate user {username}"
        )
        access_token = authenticate(username, password, authen_base_url)
        return download_one_zone(url, output_directory, check_etag, progress)
    elif status_code == 404:
        console.print(f"No zone file found for {url}")
        return False
    else:
        sys.stderr.write(
            f"Failed to download zone from {url} with code {status_code}\n"
        )
        return False


def download_zone_files(urls, working_directory, num_threads=1, check_etag=False):
    output_directory = working_directory + "/zonefiles"

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Filter URLs based on tlds and excluded_tlds config
    download_urls = []
    for link in urls:
        is_excluded = any(link.endswith("%s.zone" % tld) for tld in excluded_tlds)
        if is_excluded:
            continue

        if len(tlds):
            for tld in tlds:
                if link.endswith("%s.zone" % tld):
                    download_urls.append(link)
        else:
            download_urls.append(link)

    if num_threads > 1:
        console.print(f"Downloading with {num_threads} threads...")
        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.1f}%",
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {
                    executor.submit(
                        download_one_zone, url, output_directory, check_etag, progress
                    ): url
                    for url in download_urls
                }
                for future in as_completed(futures):
                    url = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        sys.stderr.write(f"Error downloading {url}: {e}\n")
    else:
        for link in download_urls:
            download_one_zone(link, output_directory, check_etag)


# Finally, download all zone files
start_time = datetime.datetime.now()
download_zone_files(zone_links, working_directory, args.threads, args.check_etag)
end_time = datetime.datetime.now()

console.print(
    f"{end_time}: Done. Completed downloading all zone files. Time spent: {end_time - start_time}"
)
