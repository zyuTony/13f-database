import requests
import pandas as pd
from zipfile import ZipFile
from sqlalchemy import create_engine
import os

url = 'https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2023q4_form13f.zip'
zip_path = './form13f.zip'
extract_folder = './2023q4_f13/' 
 
headers = {'Host': 'www.sec.gov', 'Connection': 'close',
         'Accept': 'application/json, text/javascript, */*; q=0.01', 'X-Requested-With': 'XMLHttpRequest',
         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
         }

# EXTRACT
response = requests.get(url, headers=headers)

if response.status_code == 200:
    with open(zip_path, 'wb') as file:
        file.write(response.content)
    print("Files downloaded successfully.")
    try:
        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        print("Files extracted successfully.")
    except zipfile.BadZipFile:
        print("Downloaded file is not a valid ZIP file.")
else:
    print(f"Failed to download. HTTP status code: {response.status_code}")

# TRANSFORM
df_SUBMISSION = pd.read_csv(extract_folder+'/SUBMISSION.tsv', delimiter='\t')
df_COVERPAGE = pd.read_csv(extract_folder+'/COVERPAGE.tsv', delimiter='\t')
df_OTHERMANAGER = pd.read_csv(extract_folder+'/OTHERMANAGER.tsv', delimiter='\t')
df_OTHERMANAGER2 = pd.read_csv(extract_folder+'/OTHERMANAGER2.tsv', delimiter='\t')
df_SIGNATURE = pd.read_csv(extract_folder+'/SIGNATURE.tsv', delimiter='\t')
df_SUMMARYPAGE = pd.read_csv(extract_folder+'/SUMMARYPAGE.tsv', delimiter='\t')
df_INFOTABLE = pd.read_csv(extract_folder+'/INFOTABLE.tsv', delimiter='\t')

nullable_cols_dict = {
    'df_SUBMISSION': [],
    'df_COVERPAGE': ["ISAMENDMENT", "AMENDMENTNO", "AMENDMENTTYPE", "CONFDENIEDEXPIRED", "DATEDENIEDEXPIRED", "DATEREPORTED",
                     "REASONFORNONCONFIDENTIALITY", "FILINGMANAGER_STREET1", "FILINGMANAGER_STREET2", "FILINGMANAGER_CITY",
                     "FILINGMANAGER_STATEORCOUNTRY", "FILINGMANAGER_ZIPCODE", "FORM13FFILENUMBER", "CRDNUMBER", "SECFILENUMBER",
                     "ADDITIONALINFORMATION"],
    'df_OTHERMANAGER': ["CIK", "FORM13FFILENUMBER", "CRDNUMBER", "SECFILENUMBER"],
    'df_OTHERMANAGER2': ["CIK", "FORM13FFILENUMBER", "CRDNUMBER", "SECFILENUMBER"],
    'df_SIGNATURE': ["PHONE"],
    'df_SUMMARYPAGE': ["OTHERINCLUDEDMANAGERSCOUNT", "TABLEENTRYTOTAL", "TABLEVALUETOTAL", "ISCONFIDENTIALOMITTED"],
    'df_INFOTABLE': ["FIGI", "PUTCALL", "OTHERMANAGER"]
}

table_names = {
    "df_SUBMISSION": df_SUBMISSION,
    "df_COVERPAGE": df_COVERPAGE,
    "df_OTHERMANAGER": df_OTHERMANAGER,
    "df_OTHERMANAGER2": df_OTHERMANAGER2,
    "df_SIGNATURE": df_SIGNATURE,
    "df_SUMMARYPAGE": df_SUMMARYPAGE,
    "df_INFOTABLE": df_INFOTABLE
}

# transform
def fill_null(df, nullable_cols=[]):
  """fill in null values for the non-nullable columns with str 'None' """
  cols_to_fill = [col for col in df.columns if col not in nullable_cols]

  for column in cols_to_fill:
    if df[column].dtype == 'float64' or df[column].dtype == 'int64':
      df[column].fillna(-1, inplace=True)
    elif df[column].dtype == 'object':
      df[column].fillna('None', inplace=True)

  return df

for df_name, df in table_names.items():
    fill_null(df, nullable_cols_dict[df_name])

