from utlis.db_utlis_sqlite import *
from utlis.os_data_utlis import download_zip, cleanup_files
'''
1. Connect to SQLite database and create tables if not exists.
2. Download zipfiles from SEC website.
3. Extract zipfiles.
4. Insert data to SQLite database.
5. Show database statistics.
'''

def main():
    # Set up connection to SQLite database
    print("Connecting to SQLite database...")
    conn = connect_to_db()
    
    if not conn:
        print("Failed to connect to database. Exiting.")
        return
    
    # Create empty tables if not exists
    print("Creating tables if they don't exist...")
    create_table_infotable(conn)
    create_table_coverpage(conn)
    create_table_signature(conn)
    create_table_summarypage(conn)
    create_table_othermanager(conn)
    create_table_othermanager2(conn)
    create_table_submission(conn)
    
    # Configuration
    zip_path = './form13f.zip'
    extract_folder_path = './form13f_tables/'
    
    # Select the data to download and insert to database
    qtrs = ["01jan2024-29feb2024", "01mar2024-31may2024", 
            "01jun2024-31aug2024",
            "01sep2024-30nov2024",
            "01dec2024-28feb2025",
            "01mar2025-31may2025"]
    
    download_url_list = ['https://www.sec.gov/files/structureddata/data/form-13f-data-sets/'+qtr+'_form13f.zip' for qtr in qtrs]
    
    # table_names = ["SUBMISSION", "COVERPAGE", "OTHERMANAGER", "OTHERMANAGER2", "SIGNATURE", "SUMMARYPAGE", "INFOTABLE"]
    table_names = ["OTHERMANAGER2", "COVERPAGE"]
    print(f"Processing {len(download_url_list)} quarters of data...")
    print("=" * 60)
    
    # Populate the tables with selected data
    for i, url in enumerate(download_url_list, 1):
        print(f"\nProcessing quarter {i}/{len(download_url_list)}: {url}")
        print("-" * 40)
        
        # Download and extract zip file
        download_zip(zip_path, extract_folder_path, url)
        
        # Insert data for each table
        for table in table_names:
            insert_data_from_tsv(conn, extract_folder_path, table)
        
        # Clean up downloaded and extracted files
        cleanup_files(zip_path, extract_folder_path)
        
        print(f"Completed quarter {i}/{len(download_url_list)}")
    
    # Show final database statistics
    show_database_stats(conn)
    
    # Close database connection
    conn.close()
    print("\nDatabase connection closed.")
    print("All data has been successfully imported to SQLite!")

if __name__ == "__main__":
    main()
