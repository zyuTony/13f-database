
import sqlite3
import csv
import os

def insert_openfigi_results_to_sqlite(csv_path="openfigi_results.csv", db_path="database/13f.sqlite"):
    """
    Insert the contents of openfigi_results.csv into a SQLite table called 'openfigi_results'.
    If the table exists, it will be dropped and recreated.
    """
    # Ensure the database directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Drop the table if it exists
    cur.execute("DROP TABLE IF EXISTS openfigi_results")

    # Create the table
    cur.execute("""
        CREATE TABLE openfigi_results (
            figi TEXT PRIMARY KEY,
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
                row['figi'],
                row['name'],
                row['ticker'],
                row['exchCode'],
                row['compositeFIGI'],
                row['securityType'],
                row['marketSector'],
                row['shareClassFIGI'],
                row['securityType2'],
                row['securityDescription']
            )
            for row in reader
        ]

    cur.executemany("""
        INSERT INTO openfigi_results (
            figi, name, ticker, exchCode, compositeFIGI, securityType, marketSector, shareClassFIGI, securityType2, securityDescription
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()
    print(f"Inserted {len(rows)} rows from {csv_path} into {db_path} (table: openfigi_results)")

# Example usage:
# insert_openfigi_results_to_sqlite()
