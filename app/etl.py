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
    order_data = {}
    
    for _, row in df.iterrows():
        status = row["status"]
        if status not in ["PAID", "SHIPPED"]:
            continue
        
        created_at = row["createdAt"]
        created_date = created_at.split("T")[0]

        sum_price = 0
        for item in row["items"]:
            qty = int(item["qty"])
            price = float(item["unitPrice"])
            sum_price += qty * price
        
        if created_date not in order_data:
            order_data[created_date] = {}
            order_data[created_date]["order_count"] = 1
            order_data[created_date]["sum_price"] = math.floor(sum_price)
        else:
            order_data[created_date]["order_count"] += 1
            order_data[created_date]["sum_price"] += math.floor(sum_price)
    
    df_t = pd.DataFrame(order_data)
    return df_t.T



def load(df_t, target_config):
    df_t.to_csv(target_config)


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