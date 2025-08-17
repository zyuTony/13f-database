# Form 13F SQLite Database Project

This project downloads SEC Form 13F data, loads it into a local SQLite database, and provides tools to view and analyze the data.

## Quick Start

1. Install requirements:

   ```
   pip install -r requirements.txt
   ```

2. Run the main script to download and load data:

   ```
   python main_sqlite.py
   ```

3. Explore the database:
   use dbeaver to connect sqllite and explore

## Files

- `main_sqlite.py` — Download and load data into SQLite.
- `view_database.py` — View and search the database.
- `utlis/` — Utility scripts.
- `queries/` — Example SQL queries.

The database file will be created in the `database/` folder.
