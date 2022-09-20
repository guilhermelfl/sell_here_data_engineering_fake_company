
from sqlalchemy import create_engine
import pandas as pd
from glob import glob
import os

def ingest_files():
    file_list = glob('reseller_files/*')

    if len(file_list) > 0:
        engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

        for file in file_list:
            print(f"ingesting file: {file}")
            new_df_to_ingest = pd.read_excel(file)
            new_df_to_ingest['file_name'] = file
            new_df_to_ingest.to_sql("external_reseller_sales", engine, if_exists = 'append', schema='external_data', index=False, chunksize = 500000)
            os.remove(file)

ingest_files()