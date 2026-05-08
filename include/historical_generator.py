import boto3
import json
import random
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def generate_historical_data(total_rows=500000):
    bucket_name = os.getenv('S3_BUCKET_NAME')
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    # Setup S3
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    
    customers = [random.randint(1000000000, 9999999999) for _ in range(1000)]
    statuses = ["COMPLETED", "PENDING", "CANCELLED"]
    start_date = datetime(2025, 1, 1) # Data dari tahun lepas
    
    file_path = "historical_transactions_500k.json"
    
    print(f"Generating {total_rows} rows locally first...")
    
    # Kita tulis ke local file dulu sebab 500k baris ni besar
    with open(file_path, 'w') as f:
        for i in range(total_rows):
            txn_date = start_date + timedelta(seconds=random.randint(0, 31536000)) # Random dlm setahun
            record = {
                "txn_id": f"TXN-{random.randint(100000, 999999)}",
                "cust_id": random.choice(customers),
                "amount": round(random.uniform(10.0, 50000.0), 2),
                "points": random.randint(0, 1000),
                "is_member": random.choice([True, False]),
                "status": random.choice(statuses),
                "txn_date": txn_date.strftime("%Y-%m-%d %H:%M:%S")
            }
            f.write(json.dumps(record) + "\n")
            
            if i % 100000 == 0:
                print(f"Progress: {i} rows done...")

    # Upload ke S3 folder historical
    s3_key = f"raw/historical/{file_path}"
    print(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}...")
    
    s3_client.upload_file(file_path, bucket_name, s3_key)
    
    # Padam fail local lepas upload
    os.remove(file_path)
    print("SUCCESS: Historical data uploaded and local file cleaned up!")

if __name__ == "__main__":
    generate_historical_data(500000)