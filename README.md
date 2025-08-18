# Form 13F SQLite Database Project

This project downloads SEC Form 13F data, stores it in a SQLite database, and provides tools for analysis. It includes CUSIP mapping using the OpenFIGI API.

## Quick Start

1. Install requirements:

   ```
   pip install -r requirements.txt
   ```

2. Run the main script:

   ```
   python main_sqlite.py
   ```

3. Explore the database using any SQLite client.

## Project Structure

### Core Scripts

- `main_sqlite.py` — Downloads and loads Form 13F data

### Data Processing for cusip data mapping

- `openfigi.py` — Queries OpenFIGI API
- `insert_cusip_mapping_csv_to_sqllite.py` — Inserts cusip mapping to db

- `cusip.py` — List of CUSIP identifiers
- `openfigi_results.csv` — Contains OpenFIGI API results

### Utilities

- `utlis/db_utlis_sqlite.py` — Database utility functions
- `utlis/os_data_utlis.py` — OS and data utilities

### Documentation

- `FORM13F_readme.htm` — SEC Form 13F documentation
- `queries/main_examples.sql` — Example SQL queries
- `queries/other_examples.sql` — Additional SQL queries

### Database

- `database/` — Contains the SQLite database file

## Features

- **Automated Data Download**: Fetches Form 13F data
- **Batch Processing**: Handles multiple ZIP files
- **CUSIP Mapping**: Uses OpenFIGI API for security info
- **SQLite Database**: Local data storage for easy querying
- **Multiple Table Support**: Manages various Form 13F tables

## Database Tables

- `SUBMISSION` - Submission info
- `COVERPAGE` - Cover page data
- `OTHERMANAGER` - Other manager info
- `OTHERMANAGER2` - Additional manager data
- `SIGNATURE` - Signature info
- `SUMMARYPAGE` - Summary data
- `INFOTABLE` - Information data
- `cusiptable` - CUSIP mapping results
