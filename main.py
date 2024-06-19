from utlis.db_utlis import *
from utlis.os_data_utlis import download_zip, cleanup_files

# variables
zip_path = './form13f.zip'
extract_folder_path = './form13f_tables/'

# qtrs = ["2013q3", "2013q4", "2014q1", "2014q2", "2014q3", "2014q4", "2015q1", "2015q2", "2015q3", "2015q4", "2016q1", "2016q2", "2016q3", "2016q4", "2017q1", "2017q2", "2017q3", "2017q4", "2018q1", "2018q2", "2018q3", "2018q4", "2019q1", "2019q2", "2019q3", "2019q4", "2020q1", "2020q2", "2020q3", "2020q4", "2021q1", "2021q2", "2021q3", "2021q4", "2022q1", "2022q2", "2022q3", "2022q4", "2023q1", "2023q2", "2023q3", "2023q4"]

# select the data to download and insert to database
qtrs = ["2021q1", "2021q2", "2021q3", "2021q4", "2022q1", "2022q2", "2022q3", "2022q4", "01jan2024-29feb2024", "01mar2024-31may2024"]
download_url_list = ['https://www.sec.gov/files/structureddata/data/form-13f-data-sets/'+qtr+'_form13f.zip' for qtr in qtrs]

# table_names = ["INFOTABLE", "COVERPAGE"]
table_names = ["SUBMISSION", "COVERPAGE", "OTHERMANAGER", "OTHERMANAGER2", "SIGNATURE", "SUMMARYPAGE", "INFOTABLE"]

# set up connection
conn = connect_to_db() 

# create empty tables if not exists
create_table_infotable(conn) 
create_table_coverpage(conn)
create_table_signature(conn) 
create_table_summarypage(conn) 
create_table_othermanager(conn) 
create_table_othermanager2(conn) 
create_table_submission(conn) 

# populate the tables with selected data
for url in download_url_list:
    download_zip(zip_path, extract_folder_path, url)  
    for table in table_names:
        insert_data_from_tsv(conn, extract_folder_path, table)  

    cleanup_files(zip_path, extract_folder_path)


