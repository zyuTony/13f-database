from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import OperationalError

# get env variables
load_dotenv()
DB_HOST = os.getenv("RDS_ENDPOINT")
DB_NAME = "postgres"
DB_USERNAME = os.getenv("RDS_USERNAME")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USERNAME,
            password=DB_PASSWORD)
        print(f"Connected to {DB_HOST} {DB_NAME}!")
        return conn
    except OperationalError as e:
        print(f"{e}")
        return None
   
def insert_data_from_tsv(conn, directory, table_name):
    cursor = conn.cursor()
    file_path = os.path.join(directory, f"{table_name}.tsv")
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    with open(file_path, 'r') as file:
        # SQL command to execute copy
        sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER E'\t';"
        try:
            cursor.copy_expert(sql, file)
            conn.commit()
            print(f"Data copied successfully for table: {table_name}")
        except Exception as e:
            conn.rollback()
            print(f"Failed to copy data for table: {table_name}. Error: {str(e)}")
    cursor.close()


def create_table_infotable(conn):
    cursor = conn.cursor()
    try:
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS infotable (
        accession_number VARCHAR(25) NOT NULL,
        infotable_sk NUMERIC(38) NOT NULL,
        nameofissuer VARCHAR(200) NOT NULL,
        titleofclass VARCHAR(150) NOT NULL,
        cusip CHAR(9) NOT NULL,
        figi VARCHAR(17),
        value NUMERIC(12) NOT NULL,
        sshprnamt NUMERIC(16) NOT NULL,
        sshprnamttype VARCHAR(10) NOT NULL,
        putcall VARCHAR(10),
        investmentdiscretion VARCHAR(10) NOT NULL,
        othermanager VARCHAR(100),
        voting_auth_sole NUMERIC(16) NOT NULL,
        voting_auth_shared NUMERIC(16) NOT NULL,
        voting_auth_none NUMERIC(16) NOT NULL,
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
        accession_number VARCHAR(25) NOT NULL,
        reportcalendarorquarter DATE NOT NULL,
        isamendment CHAR(1),
        amendmentno SMALLINT,
        amendmenttype VARCHAR(20),
        confdeniedexpired CHAR(1),
        datedeniedexpired DATE,
        datereported DATE,
        reasonfornonconfidentiality VARCHAR(40),
        filingmanager_name VARCHAR(150) NOT NULL,
        filingmanager_street1 VARCHAR(40),
        filingmanager_street2 VARCHAR(40),
        filingmanager_city VARCHAR(30),
        filingmanager_stateorcountry CHAR(2),
        filingmanager_zipcode VARCHAR(10),
        reporttype VARCHAR(30) NOT NULL,
        form13ffilenumber VARCHAR(17),
        crdnumber VARCHAR(17),
        secfilenumber VARCHAR(17),
        provideinfoforinstructions CHAR(1) NOT NULL,
        additionalinformation VARCHAR(4000),
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
        accession_number VARCHAR(25) NOT NULL,
        name VARCHAR(150) NOT NULL,
        title VARCHAR(60) NOT NULL,
        phone VARCHAR(20),
        signature VARCHAR(150),
        city VARCHAR(150) NOT NULL,
        stateorcountry CHAR(2) NOT NULL,
        signaturedate DATE NOT NULL,
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
        accession_number VARCHAR(25) NOT NULL,
        otherincludedmanagerscount NUMERIC(3),
        tableentrytotal NUMERIC(6),
        tablevaluetotal NUMERIC(13),
        isconfidentialomitted CHAR(1),
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
        accession_number VARCHAR(25) NOT NULL,
        othermanager_sk NUMERIC(38) NOT NULL,
        cik VARCHAR(10),
        form13ffilenumber VARCHAR(17),
        crdnumber VARCHAR(17),
        secfilenumber VARCHAR(17),
        name VARCHAR(150) NOT NULL,
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
        accession_number VARCHAR(25) NOT NULL,
        sequence_number NUMERIC(3) NOT NULL,
        cik VARCHAR(10),
        form13ffilenumber VARCHAR(17),
        crdnumber VARCHAR(17),
        secfilenumber VARCHAR(17),
        name VARCHAR(150) NOT NULL,
        PRIMARY KEY (accession_number, sequence_number, name)
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
        accession_number VARCHAR(25) NOT NULL,
        filing_date DATE NOT NULL,
        submissiontype VARCHAR(10) NOT NULL,
        cik VARCHAR(10) NOT NULL,
        periodofreport DATE NOT NULL,
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