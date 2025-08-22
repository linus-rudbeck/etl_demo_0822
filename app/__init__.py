from app.etl import etl_job

def run():
    source_config = {
        "urls": [
            f"https://distansakademin.github.io/api/orders/{i}.json" for i in range(1,6)
            ]
    }
    
    etl_job(source_config, None)