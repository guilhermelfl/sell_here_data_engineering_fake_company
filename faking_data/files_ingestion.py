
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

def ingest_weblog_files():
    file_list = glob('weblog_files/*')
    if len(file_list) > 0:
        engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

        for file_path in file_list:
            file = open(f'{file_path}', 'r')
            list_dict = []
            for line in file.readlines():
                line_dict = {}
                lines_list = line.split(sep=' ', maxsplit=9)
                line_dict['client_ip'] = lines_list[0]
                line_dict['identifier'] = lines_list[1]
                line_dict['user_name'] = lines_list[2]
                line_dict['log_date_time'] = lines_list[3] + ' ' + lines_list[4]
                line_dict['request_line'] = lines_list[5]
                line_dict['server_response'] = lines_list[6]
                line_dict['response_size_in_bytes'] = lines_list[7]
                line_dict['referer'] = lines_list[8]
                line_dict['user_agent'] = lines_list[9]
                list_dict.append(line_dict)
            log_df = pd.DataFrame(list_dict)
            log_df.to_sql("web_app_logs", engine, if_exists = 'append', schema='external_data', index=False, chunksize = 500000)
            file.close()
            os.remove(file_path)

ingest_weblog_files()
ingest_files()
