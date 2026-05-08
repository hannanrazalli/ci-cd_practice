from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import io
import pandas as pd
import pandas_gbq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def load_to_bigquery(ds, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    bucket = 'datalake-raw-1-212105053682-ap-southeast-1-an'
    
    dt = datetime.strptime(ds, '%Y-%m-%d')
    year, month, day = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')
    
    all_dfs = []

    # --- 1. PROSES LANDING (IKUT TARIKH) ---
    landing_key = f"raw/landing/year={year}/month={month}/day={day}/daily_transactions_{ds}.json"
    print(f"Checking landing: {landing_key}")
    
    if s3_hook.check_for_key(landing_key, bucket):
        content = s3_hook.read_key(landing_key, bucket)
        df_landing = pd.read_json(io.StringIO(content), lines=True)
        all_dfs.append(df_landing)
        print(f"Found landing data: {len(df_landing)} rows.")

    # --- 2. PROSES HISTORICAL (HANYA PADA 1 MEI) ---
    if ds == '2026-05-01':
        historical_key = 'raw/historical/historical_transactions_500k.json'
        print(f"Checking historical: {historical_key}")
        
        if s3_hook.check_for_key(historical_key, bucket):
            print("Found historical file. Reading 500k rows (this might take a while)...")
            # Gunakan chunksize jika laptop rasa berat, tapi untuk 500k pandas masih ok.
            h_content = s3_hook.read_key(historical_key, bucket)
            df_hist = pd.read_json(io.StringIO(h_content), lines=True)
            all_dfs.append(df_hist)
            print(f"Found historical data: {len(df_hist)} rows.")

    if not all_dfs:
        print(f"No data found for {ds} in landing or historical.")
        return

    # Gabung semua
    final_df = pd.concat(all_dfs, ignore_index=True)

    # --- 3. CLEANING & SCHEMA FIX ---
    # Pastikan column wujud sebelum masuk BQ
    for col in ['points', 'is_member']:
        if col not in final_df.columns:
            final_df[col] = 0 if col == 'points' else False

    # Force types supaya tak error kat BigQuery
    final_df['txn_id'] = final_df['txn_id'].astype(str)
    final_df['cust_id'] = final_df['cust_id'].astype(str)
    final_df['points'] = final_df['points'].fillna(0).astype(int)
    final_df['is_member'] = final_df['is_member'].fillna(False).astype(bool)

    # --- 4. LOAD KE BIGQUERY ---
    print(f"Uploading total {len(final_df)} rows to BigQuery...")
    pandas_gbq.to_gbq(
        final_df,
        destination_table='bronze.raw_transactions',
        project_id='transactions-practice',
        if_exists='append',
        credentials=bq_hook.get_credentials(),
        progress_bar=False
    )
    print("Ingestion successful.")

# --- DEFINISI DAG (MAINTAIN SATU NAMA SAHAJA) ---
with DAG(
    dag_id='s3_to_bigquery',
    start_date=datetime(2026, 5, 1),
    schedule='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['production', 'bronze']
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_s3_to_bigquery_task',
        python_callable=load_to_bigquery
    )