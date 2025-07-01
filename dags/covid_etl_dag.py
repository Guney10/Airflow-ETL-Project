from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

def extract_covid_data():
    url = "https://disease.sh/v3/covid-19/countries"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    df.to_csv("/opt/airflow/dags/output/covid_data.csv", index=False)

def transform_data():
    df = pd.read_csv('/opt/airflow/dags/output/covid_data.csv')

    # Convert 'countryInfo' from string to dictionary
    df['countryInfo'] = df['countryInfo'].apply(eval)

    # Extract relevant fields from the countryInfo column
    df['iso2'] = df['countryInfo'].apply(lambda x: x.get('iso2'))
    df['iso3'] = df['countryInfo'].apply(lambda x: x.get('iso3'))
    df['lat'] = df['countryInfo'].apply(lambda x: x.get('lat'))
    df['long'] = df['countryInfo'].apply(lambda x: x.get('long'))

    # Select and reorder desired columns
    selected_columns = [
        'country', 'iso2', 'iso3', 'lat', 'long', 'cases',
        'deaths', 'recovered', 'population',
    ]
    df = df[selected_columns]

    # Clean data: fill NaNs, convert types if needed
    df.fillna({'cases': 0, 'deaths': 0, 'recovered': 0, 'population': 0}, inplace=True)
    df[['cases', 'deaths', 'recovered', 'population']] = df[['cases', 'deaths', 'recovered', 'population']].astype(int)

    # Add fatality rate
    df['fatality_rate'] = df.apply(
        lambda row: round((row['deaths'] / row['cases']) * 100, 2) if row['cases'] > 0 else 0,
        axis=1
    )

    df.to_csv('/opt/airflow/dags/output/covid_data_transformed.csv', index=False)
    print("The transformation is complete.")    


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='covid_api_etl',
    default_args=default_args,
    description='ETL DAG for COVID-19 data using Airflow',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_covid_data',
        python_callable=extract_covid_data
    )
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    extract_task >> transform_task
