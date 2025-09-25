import threading
import signal
import os
import sys
import time
import requests
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Referer": "https://www.bybit.com/derivatives/en/history-data",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Connection": "keep-alive",
    "Priority": "u=0",
    "TE": "trailers"
}


cookies = {
}


terminate = threading.Event()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Bybit orderbook data."
    )
    parser.add_argument("symbol", nargs="?", default="BTCPERP", help="Symbol to download (default: BTCPERP)")
    parser.add_argument("--start-date", type=str, default="2023-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=datetime.today().strftime("%Y-%m-%d"), help="End date (YYYY-MM-DD)")
    parser.add_argument("--window-size", type=int, default=7, help="Window size in days (default: 7)")
    return parser.parse_args()


def download_file(url, filename, headers=None, cookies=None, max_retries=5):
    for attempt in range(max_retries):
        if terminate.is_set():
            print(f"Terminating download: {filename}")
            return False
        try:
            with requests.get(url, headers=headers, cookies=cookies, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if terminate.is_set():
                            print(f"Terminating download: {filename}")
                            return False
                        if chunk:
                            f.write(chunk)
            print(f"Saved {filename}")
            return True
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError) as e:
            print(f"Download failed ({e}), retrying {attempt + 1}/{max_retries}...")
            time.sleep(2)
        except Exception as e:
            print(f"Other error: {e}")
            break
    print(f"Failed to download {filename} after {max_retries} attempts.")
    return False


def handle_exit(signum, frame):
    print("\nTermination signal received. Exiting gracefully.")
    terminate.set()
    sys.exit(0)


def daterange(start_date, end_date, step_days):
    current = start_date
    while current <= end_date:
        window_end = min(current + timedelta(days=step_days - 1), end_date)
        yield current, window_end
        current = window_end + timedelta(days=1)


signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)
try:
    signal.signal(signal.SIGQUIT, handle_exit)
except AttributeError:
    pass


if __name__ == "__main__":
    args = parse_args()
    symbol = args.symbol
    output_dir = os.path.join("data", symbol)
    os.makedirs(output_dir, exist_ok=True)

    overall_start = datetime.strptime(args.start_date, "%Y-%m-%d")
    overall_end = datetime.strptime(args.end_date, "%Y-%m-%d")
    total_days = (overall_end - overall_start).days + 1

    window_size = min(args.window_size, total_days)

    for start, end in daterange(overall_start, overall_end, window_size):
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        url = (
            "https://www.bybit.com/x-api/quote/public/support/download/list-files"
            f"?bizType=contract&productId=orderbook&symbols={symbol}&interval=daily"
            f"&periods=&startDay={start_str}&endDay={end_str}"
        )
        print(f"\nRequesting {start_str} to {end_str}")
        response = requests.get(url, headers=headers, cookies=cookies)
        try:
            response_json = response.json()
        except Exception as e:
            print(f"Failed to parse JSON for {start_str} to {end_str}: {e}")
            continue

        file_list = response_json.get("result", {}).get("list", [])
        if not file_list:
            print(f"No files found for {start_str} to {end_str}")
            continue

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for file_info in file_list:
                file_url = file_info["url"]
                filename = file_info["filename"]
                save_path = os.path.join(output_dir, filename)
                print(f"Queueing {save_path} ...")
                futures.append(executor.submit(download_file, file_url, save_path, headers, cookies))
            for future in as_completed(futures):
                future.result()
