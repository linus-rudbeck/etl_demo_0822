from datetime import datetime
import pandas as pd
import math


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
    orders_per_date = {}
    sum_price_per_date = {}
    
    for _, row in df.iterrows():
        status = row["status"]
        if status not in ["PAID", "SHIPPED"]:
            continue
        
        created_at = row["createdAt"]
        created_date = created_at.split("T")[0]
        
        if created_date not in orders_per_date:
            orders_per_date[created_date] = 1
        else:
            orders_per_date[created_date] += 1
                
        sum_price = 0
        for item in row["items"]:
            qty = int(item["qty"])
            price = float(item["unitPrice"])
            sum_price += qty * price
        
        if created_date not in sum_price_per_date:
            sum_price_per_date[created_date] = math.floor(sum_price)
        else:
            sum_price_per_date[created_date] += math.floor(sum_price)
    
    print(orders_per_date, sum_price_per_date)



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