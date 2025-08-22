from datetime import datetime
import pandas as pd

def extract(source_config):
    df_arr = []
    urls = source_config["urls"]
    for url in urls:
        df_url = pd.read_json(url)
        df_arr.append(df_url)
    
    df = pd.concat(df_arr)
    df.to_csv("orders.csv")
    
    return df

def transform(df):
    # Beräkna:
    # - Antal beställningar per dag
    # - Summa (pris) per dag
    # (Inkludera inte beställningar som avbrutits)
    pass

def load(df_t, target_config):
    pass


def etl_job(source_config, target_config):
    start = datetime.now()
    print(f"ETL start {start}")
    
    try:
        raw = extract(source_config)
        df_t = transform(raw)
        result = load(df_t, target_config)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error: {e}")