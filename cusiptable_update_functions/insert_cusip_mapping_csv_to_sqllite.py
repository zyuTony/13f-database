
import sqlite3
import csv
import os

def insert_openfigi_results_to_sqlite(csv_path="openfigi_results.csv", db_path="database/form13f_data.db"):
    """
    Insert the contents of openfigi_results.csv into a SQLite table called 'openfigi_results'.
    If the table exists, it will be dropped and recreated.

    The new openfigi_results.csv has the following columns:
    cusip,figi,name,ticker,exchCode,compositeFIGI,securityType,marketSector,shareClassFIGI,securityType2,securityDescription
    """
    # Ensure the database directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Drop the table if it exists
    cur.execute("DROP TABLE IF EXISTS cusiptable")

    # Create the table with the new schema (cusip as PRIMARY KEY)
    cur.execute("""
        CREATE TABLE cusiptable (
            cusip TEXT PRIMARY KEY,
            figi TEXT,
            name TEXT,
            ticker TEXT,
            exchCode TEXT,
            compositeFIGI TEXT,
            securityType TEXT,
            marketSector TEXT,
            shareClassFIGI TEXT,
            securityType2 TEXT,
            securityDescription TEXT
        )
    """)

    # Read the CSV and insert rows
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = [
            (
                row.get('cusip', ''),
                row.get('figi', ''),
                row.get('name', ''),
                row.get('ticker', ''),
                row.get('exchCode', ''),
                row.get('compositeFIGI', ''),
                row.get('securityType', ''),
                row.get('marketSector', ''),
                row.get('shareClassFIGI', ''),
                row.get('securityType2', ''),
                row.get('securityDescription', '')
            )
            for row in reader
        ]

    cur.executemany("""
        INSERT INTO cusiptable (
            cusip, figi, name, ticker, exchCode, compositeFIGI, securityType, marketSector, shareClassFIGI, securityType2, securityDescription
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()
    print(f"Inserted {len(rows)} rows from {csv_path} into {db_path} (table: openfigi_results)")

# Example usage:
insert_openfigi_results_to_sqlite()
