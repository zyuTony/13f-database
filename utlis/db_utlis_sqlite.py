import sqlite3
import os
import csv

def connect_to_db():
    """
    Connect to SQLite database. Creates the database file if it doesn't exist.
    """
    try:
        # Create database directory if it doesn't exist
        db_dir = './database'
        os.makedirs(db_dir, exist_ok=True)
        
        db_path = os.path.join(db_dir, 'form13f_data.db')
        conn = sqlite3.connect(db_path)
        
        # Enable foreign keys and set journal mode for better performance
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        
        print(f"Connected to SQLite database: {db_path}")
        return conn
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return None

def insert_data_from_tsv(conn, directory, table_name):
    """
    Insert data from TSV file into SQLite table
    """
    cursor = conn.cursor()
    file_path = os.path.join(directory, f"{table_name}.tsv")
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Read TSV file
            tsv_reader = csv.reader(file, delimiter='\t')
            headers = next(tsv_reader)  # Skip header row
            
            # Prepare INSERT statement
            placeholders = ','.join(['?' for _ in headers])
            columns = ','.join(headers)
            sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Insert data row by row
            row_count = 0
            for row in tsv_reader:
                if len(row) == len(headers):  # Ensure row has correct number of columns
                    cursor.execute(sql, row)
                    row_count += 1
                    
                    # Commit every 1000 rows for better performance
                    if row_count % 1000 == 0:
                        conn.commit()
                        # print(f"Inserted {row_count} rows for {table_name}...")
            
            # Final commit
            conn.commit()
            print(f"Data inserted successfully for table: {table_name}. Total rows: {row_count}")
            
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert data for table: {table_name}. Error: {str(e)}")
    finally:
        cursor.close()

def create_table_infotable(conn):
    cursor = conn.cursor()
    try:
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS infotable (
        accession_number TEXT NOT NULL,
        infotable_sk INTEGER NOT NULL,
        nameofissuer TEXT NOT NULL,
        titleofclass TEXT NOT NULL,
        cusip TEXT NOT NULL,
        figi TEXT,
        value INTEGER NOT NULL,
        sshprnamt INTEGER NOT NULL,
        sshprnamttype TEXT NOT NULL,
        putcall TEXT,
        investmentdiscretion TEXT NOT NULL,
        othermanager TEXT,
        voting_auth_sole INTEGER NOT NULL,
        voting_auth_shared INTEGER NOT NULL,
        voting_auth_none INTEGER NOT NULL,
        PRIMARY KEY (accession_number, infotable_sk)
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        print(f"infotable created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_table_coverpage(conn):
    cursor = conn.cursor()
    try:
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS coverpage (
        accession_number TEXT NOT NULL,
        reportcalendarorquarter TEXT NOT NULL,
        isamendment TEXT,
        amendmentno INTEGER,
        amendmenttype TEXT,
        confdeniedexpired TEXT,
        datedeniedexpired TEXT,
        datereported TEXT,
        reasonfornonconfidentiality TEXT,
        filingmanager_name TEXT NOT NULL,
        filingmanager_street1 TEXT,
        filingmanager_street2 TEXT,
        filingmanager_city TEXT,
        filingmanager_stateorcountry TEXT,
        filingmanager_zipcode TEXT,
        reporttype TEXT NOT NULL,
        form13ffilenumber TEXT,
        crdnumber TEXT,
        secfilenumber TEXT,
        provideinfoforinstruction5 TEXT NOT NULL,
        additionalinformation TEXT,
        PRIMARY KEY (accession_number)
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        print(f"coverpage created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_table_signature(conn):
    cursor = conn.cursor()
    try:
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS signature (
        accession_number TEXT NOT NULL,
        name TEXT NOT NULL,
        title TEXT NOT NULL,
        phone TEXT,
        signature TEXT,
        city TEXT NOT NULL,
        stateorcountry TEXT NOT NULL,
        signaturedate TEXT NOT NULL,
        PRIMARY KEY (accession_number)
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        print(f"signature created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_table_summarypage(conn):
    cursor = conn.cursor()
    try:
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS summarypage (
        accession_number TEXT NOT NULL,
        otherincludedmanagerscount INTEGER,
        tableentrytotal INTEGER,
        tablevaluetotal INTEGER,
        isconfidentialomitted TEXT,
        PRIMARY KEY (accession_number)
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        print(f"summarypage created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_table_othermanager(conn):
    cursor = conn.cursor()
    try:
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS othermanager (
        accession_number TEXT NOT NULL,
        othermanager_sk INTEGER NOT NULL,
        cik TEXT,
        form13ffilenumber TEXT,
        crdnumber TEXT,
        secfilenumber TEXT,
        name TEXT NOT NULL,
        PRIMARY KEY (accession_number, othermanager_sk)
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        print(f"othermanager created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_table_othermanager2(conn):
    cursor = conn.cursor()
    try:
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS othermanager2 (
        accession_number TEXT NOT NULL,
        sequencenumber INTEGER NOT NULL,
        cik TEXT,
        form13ffilenumber TEXT,
        crdnumber TEXT,
        secfilenumber TEXT,
        name TEXT NOT NULL,
        PRIMARY KEY (accession_number, sequencenumber, name)
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        print(f"othermanager2 created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_table_submission(conn):
    cursor = conn.cursor()
    try:
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS submission (
        accession_number TEXT NOT NULL,
        filing_date TEXT NOT NULL,
        submissiontype TEXT NOT NULL,
        cik TEXT NOT NULL,
        periodofreport TEXT NOT NULL,
        PRIMARY KEY (accession_number)
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        print(f"submission created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def get_table_info(conn, table_name):
    """
    Get information about a table (column names and types)
    """
    cursor = conn.cursor()
    try:
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        print(f"\nTable: {table_name}")
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
    except Exception as e:
        print(f"Error getting table info for {table_name}: {e}")
    finally:
        cursor.close()

def get_row_count(conn, table_name):
    """
    Get the number of rows in a table
    """
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"{table_name}: {count} rows")
        return count
    except Exception as e:
        print(f"Error getting row count for {table_name}: {e}")
        return 0
    finally:
        cursor.close()

def show_database_stats(conn):
    """
    Show statistics for all tables in the database
    """
    print("\n" + "="*50)
    print("DATABASE STATISTICS")
    print("="*50)
    
    tables = ["infotable", "coverpage", "signature", "summarypage", "othermanager", "othermanager2", "submission"]
    total_rows = 0
    
    for table in tables:
        count = get_row_count(conn, table)
        total_rows += count
    
    print("-" * 50)
    print(f"TOTAL ROWS: {total_rows}")
    print("="*50)
