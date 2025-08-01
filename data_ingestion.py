import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
logging.basicConfig(
    filename = "logs/ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df , table_name , engine):
    df.to_sql(table_name , con = engine , if_exists = 'replace' , index = False)
    
def load_raw_data():
    
    for file in os.listdir('data'):
        start_time = time.time()
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            print(df.shape)
            logging.info(f"ingestion {file} in db")
            ingest_db(df , file[:-4]  , engine)
    end_time = time.time()    
    total_time = (end_time - start_time)/60
    logging.info('Ingestion Complete')
    logging.info(f'\n Total Time Taken: {total_time} minutes')    
    
if __name__ == '__main__'    :
    load_raw_data()