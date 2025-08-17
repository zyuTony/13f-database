import os
import zipfile
import glob
from utlis.db_utlis_sqlite import *
from utlis.os_data_utlis import cleanup_files

def process_zip_file(conn, zip_file_path, extract_folder_path, table_names):
    """
    Process a single zip file: extract it and insert data from TSV files
    
    Args:
        conn: SQLite database connection
        zip_file_path: Path to the zip file
        extract_folder_path: Directory to extract files to
        table_names: List of table names to process
    """
    print(f"Processing zip file: {zip_file_path}")
    
    try:
        # Extract the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder_path)
        print(f"Extracted {zip_file_path} successfully.")
        
        # Insert data for each table
        for table in table_names:
            insert_data_from_tsv(conn, extract_folder_path, table)
        
        # Clean up extracted files (but keep the zip file)
        cleanup_files(None, extract_folder_path)
        print(f"Completed processing {zip_file_path}")
        
    except zipfile.BadZipFile:
        print(f"Error: {zip_file_path} is not a valid ZIP file.")
    except Exception as e:
        print(f"Error processing {zip_file_path}: {str(e)}")

def process_all_zips_in_folder(conn, zip_folder_path, extract_base_path, table_names):
    """
    Process all zip files in a specified folder
    
    Args:
        conn: SQLite database connection
        zip_folder_path: Folder containing zip files
        extract_base_path: Base directory for extracting files
        table_names: List of table names to process
    """
    # Find all zip files in the folder
    zip_pattern = os.path.join(zip_folder_path, "*.zip")
    zip_files = glob.glob(zip_pattern)
    
    if not zip_files:
        print(f"No zip files found in {zip_folder_path}")
        return
    
    print(f"Found {len(zip_files)} zip files to process")
    
    # Process each zip file
    for i, zip_file in enumerate(zip_files, 1):
        print(f"\nProcessing zip file {i}/{len(zip_files)}")
        print("-" * 50)
        
        # Create a unique extract folder for each zip file
        zip_name = os.path.splitext(os.path.basename(zip_file))[0]
        extract_folder_path = os.path.join(extract_base_path, zip_name)
        
        # Ensure extract directory exists
        os.makedirs(extract_folder_path, exist_ok=True)
        
        # Process the zip file
        process_zip_file(conn, zip_file, extract_folder_path, table_names)
    
    print("\n" + "=" * 60)
    print("All zip files processed!")

def main():
    """
    Main function to process all zip files in a folder using SQLite
    """
    # Set up SQLite database connection
    print("Connecting to SQLite database...")
    conn = connect_to_db()
    if not conn:
        print("Failed to connect to database. Exiting.")
        return
    
    # Create tables if they don't exist
    print("Creating tables if they don't exist...")
    create_table_infotable(conn)
    create_table_coverpage(conn)
    create_table_signature(conn)
    create_table_summarypage(conn)
    create_table_othermanager(conn)
    create_table_othermanager2(conn)
    create_table_submission(conn)
    
    # Configuration
    zip_folder_path = './form13f_zip'  # Folder containing zip files
    extract_base_path = './form13f_tables'  # Base directory for extracting files
    table_names = ["SUBMISSION", "COVERPAGE", "OTHERMANAGER", "OTHERMANAGER2", "SIGNATURE", "SUMMARYPAGE", "INFOTABLE"]
    
    # Ensure directories exist
    os.makedirs(zip_folder_path, exist_ok=True)
    os.makedirs(extract_base_path, exist_ok=True)
    
    print(f"Processing all zip files in: {zip_folder_path}")
    print(f"Extracting to base directory: {extract_base_path}")
    print(f"Processing tables: {table_names}")
    print("=" * 60)
    
    # Process all zip files
    process_all_zips_in_folder(conn, zip_folder_path, extract_base_path, table_names)
    
    # Show final database statistics
    show_database_stats(conn)
    
    # Close database connection
    conn.close()
    print("\nDatabase connection closed.")
    print("All data has been successfully imported to SQLite!")

if __name__ == "__main__":
    main()
