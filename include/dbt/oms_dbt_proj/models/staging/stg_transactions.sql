{%- set cfg = var('stg_tables')['stg_transactions'] -%}
{%- set stg_cols = cfg['columns'] -%}

{{ config(
    materialized='incremental',
    unique_key='txn_id'
) }}

WITH raw_data AS (
    SELECT *
    FROM {{ source('transactions_source', 'raw_transactions') }}
)

SELECT
    {% for cols in stg_cols %}
        {{ cols }}{% if not loop.last %}, {% endif %}
    {% endfor %},
    CASE
        WHEN txn_id IS NULL OR txn_id = '' THEN 'CORRUPT'
        WHEN cust_id IS NULL OR SAFE_CAST(cust_id AS INT64) < 0 THEN 'CORRUPT'
        WHEN amount IS NULL OR SAFE_CAST(amount AS FLOAT64) < 0 THEN 'CORRUPT'
        WHEN points IS NULL OR SAFE_CAST(points AS INT64) < 0 THEN 'CORRUPT'
        WHEN is_member IS NULL THEN 'CORRUPT'
        WHEN status IS NULL OR status = '' THEN 'CORRUPT'
        WHEN txn_date IS NULL THEN 'CORRUPT'
        ELSE 'CLEAN'
    END AS _record_status,
    {{ audit_columns('bronze') }}
FROM raw_data