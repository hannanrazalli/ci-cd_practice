import boto3
import json
import random
import io
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load fail .env dari folder utama project
load_dotenv()

def run_s3_ingestion(execution_date):
    # Ambil konfigurasi dari environment variables
    bucket_name = os.getenv('S3_BUCKET_NAME')
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_DEFAULT_REGION', 'ap-southeast-1')

    # Semak jika variable None (untuk debug)
    if not bucket_name or not access_key:
        print(f"ERROR: Variable Not Found Bucket: {bucket_name}, Key: {access_key}")
        return

    # 1. Logik Tarikh (Penting untuk Folder Partition)
    dt = datetime.strptime(execution_date, '%Y-%m-%d')
    year, month, day = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')
    
    # 2. Generate Data (Structure Banking/Finance)
    customers = [random.randint(1000000000, 9999999999) for _ in range(50)]
    statuses = ["COMPLETED", "PENDING", "CANCELLED"]
    
    data_list = []
    for i in range(15):  # Kita buat 15 transaksi simulasi harian
        data_list.append({
            "txn_id": f"TXN-{random.randint(10000, 99999)}",
            "cust_id": random.choice(customers),
            "amount": round(random.uniform(10.50, 15000.75), 2) if i % 15 != 0 else None,
            "points": random.randint(0, 500),
            "is_member": random.choice([True, False]),
            "status": random.choice(statuses),
            "txn_date": (dt + timedelta(seconds=random.randint(0, 86400))).strftime("%Y-%m-%d %H:%M:%S")
        })

    # 3. Tukar ke JSON Lines (Format standard BigQuery)
    json_lines = "\n".join([json.dumps(record) for record in data_list])
    
    # 4. Tentukan Path S3 (Hive-style Partitioning)
    s3_key = f"raw/landing/year={year}/month={month}/day={day}/daily_transactions_{execution_date}.json"
    
    # 5. Upload ke S3 menggunakan Boto3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    
    try:
        print(f"Memulakan upload ke: s3://{bucket_name}/{s3_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_lines
        )
        print("SUCCESS: Successful ingestion to S3!")
    except Exception as e:
        print(f"ERROR: Ingestion error: {str(e)}")

if __name__ == "__main__":
    # Test lari secara manual untuk tarikh hari ini
    run_s3_ingestion("2026-05-07")