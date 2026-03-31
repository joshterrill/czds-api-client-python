import requests
import time
import sys

MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _make_request(method, url, access_token, stream=False):
    bearer_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": "Bearer {0}".format(access_token),
    }

    backoff = INITIAL_BACKOFF
    last_exception = None

    for attempt in range(MAX_RETRIES):
        try:
            if method == "GET":
                response = requests.get(
                    url, params=None, headers=bearer_headers, stream=stream
                )
            elif method == "HEAD":
                response = requests.head(url, headers=bearer_headers)
            else:
                raise ValueError(f"Unsupported method: {method}")

            if response.status_code not in RETRYABLE_STATUS_CODES:
                return response

            # Retryable status code
            if attempt < MAX_RETRIES - 1:
                sys.stderr.write(
                    f"Request to {url} returned {response.status_code}, retrying in {backoff}s (attempt {attempt + 1}/{MAX_RETRIES})\n"
                )
                time.sleep(backoff)
                backoff *= 2
            else:
                return response

        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < MAX_RETRIES - 1:
                sys.stderr.write(
                    f"Request to {url} failed: {e}, retrying in {backoff}s (attempt {attempt + 1}/{MAX_RETRIES})\n"
                )
                time.sleep(backoff)
                backoff *= 2
            else:
                raise last_exception

    return response


def do_get(url, access_token):
    return _make_request("GET", url, access_token, stream=True)


def do_head(url, access_token):
    return _make_request("HEAD", url, access_token)
