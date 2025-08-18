import requests
import csv
import os
import time
from cusiptable_update_functions.cusip import cusips


def get_and_write_openfigi_info(cusips, csv_filename="openfigi_results.csv", batch_size=5, save_every=10, sleep_seconds=5):
    """
    For a list of CUSIPs, query OpenFIGI API and write results to a CSV file.
    If a CUSIP already exists in the CSV, its row will be replaced.
    Periodically saves the CSV and sleeps between requests.
    Also saves the original CUSIP in the CSV.
    """
    url = "https://api.openfigi.com/v3/mapping"
    headers = {
        'Content-Type': 'application/json'
        # Optionally add 'X-OPENFIGI-APIKEY': 'YOUR_API_KEY'
    }
    fieldnames = [
        'cusip',  # Add original CUSIP as the first column
        'figi',
        'name',
        'ticker',
        'exchCode',
        'compositeFIGI',
        'securityType',
        'marketSector',
        'shareClassFIGI',
        'securityType2',
        'securityDescription'
    ]

    # Step 1: Read existing CSV into a dict keyed by CUSIP (ticker or figi)
    existing_rows = {}
    if os.path.isfile(csv_filename):
        with open(csv_filename, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Try to get the CUSIP from the row, fallback to ticker/figi if not present
                key = row.get('cusip', '') or row.get('ticker', '') or row.get('figi', '')
                if key:
                    existing_rows[key] = row

    # Step 2: Query OpenFIGI API in batches, periodically save and sleep
    batches_since_save = 0
    for i in range(0, len(cusips), batch_size):
        batch = cusips[i:i+batch_size]
        payload = [{"idType": "ID_CUSIP", "idValue": c} for c in batch]
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Error fetching info for CUSIPs {batch}: {e}")
            time.sleep(sleep_seconds)
            continue

        # Step 3: Update or add rows in the dict
        for idx, result in enumerate(data):
            info = result.get('data', [{}])
            cusip_val = batch[idx] if idx < len(batch) else ""
            if info and isinstance(info, list) and info[0]:
                row = {key: info[0].get(key, "") for key in fieldnames if key != 'cusip'}
                row['cusip'] = cusip_val
                key = cusip_val
                if key:
                    existing_rows[key] = row
            else:
                # Optionally, add a row with only the CUSIP if not found
                row = {k: "" for k in fieldnames}
                row['cusip'] = cusip_val
                existing_rows[cusip_val] = row

        batches_since_save += 1

        # Periodically save to CSV
        if batches_since_save >= save_every:
            with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in existing_rows.values():
                    writer.writerow(row)
            print(f"Progress saved after {i+batch_size} CUSIPs.")
            batches_since_save = 0

        # Sleep between requests
        time.sleep(sleep_seconds)

    # Step 4: Write all rows back to CSV (final save)
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in existing_rows.values():
            writer.writerow(row)
    print("Final save complete.")

# Usage:
get_and_write_openfigi_info(cusips)
