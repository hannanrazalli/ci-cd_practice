FROM astrocrpublic.azurecr.io/runtime:3.2-3

RUN pip install --no-cache-dir \
    apache-airflow-providers-google==16.0.0 \
    apache-airflow-providers-amazon==9.0.0