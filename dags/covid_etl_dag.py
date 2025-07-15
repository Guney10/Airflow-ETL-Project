from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
# Grab the API 
def extract_covid_data():
    url = "https://disease.sh/v3/covid-19/countries"
    response = requests.get(url) #Send a GET request to the API and store response
    data = response.json()
    df = pd.DataFrame(data) # convert JSON into pandas DataFrame
    df.to_csv("/opt/airflow/dags/output/covid_data.csv", index=False) #Save as CSV file in the specified output directory

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

    # Clean data, fill nulls, convert types if needed
    df.fillna({'cases': 0, 'deaths': 0, 'recovered': 0, 'population': 0}, inplace=True)
    df[['cases', 'deaths', 'recovered', 'population']] = df[['cases', 'deaths', 'recovered', 'population']].astype(int)

    # Add fatality rate for analysis
    df['fatality_rate'] = df.apply(
        lambda row: round((row['deaths'] / row['cases']) * 100, 2) if row['cases'] > 0 else 0,
        axis=1
    )

    df.to_csv('/opt/airflow/dags/output/covid_data_transformed.csv', index=False) #DataFrame to a CSV file in the DAGs output directory
    print("The transformation is complete.")    

def load_data():
    df = pd.read_csv('/opt/airflow/dags/output/covid_data_transformed.csv') #Read the transformed CSV file into a DataFrame

    conn = psycopg2.connect(
        host="postgres", # This matches the service name in docker-compose
        database="airflow", # Database name
        user="airflow", # Database user
        password="airflow" # Database password
    )
    cur = conn.cursor()

    # Optional: Clear old data before insert
    cur.execute("TRUNCATE TABLE covid_stats;")

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO covid_stats (country, iso2, iso3, lat, long, cases, deaths, recovered, population, fatality_rate)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['country'],
            row['iso2'],
            row['iso3'],
            row['lat'],
            row['long'],
            row['cases'],
            row['deaths'],
            row['recovered'],
            row['population'],
            row['fatality_rate']
        ))
    # Finalise the database transaction and clean up connections
    conn.commit()
    cur.close()
    conn.close()
    print("Data loaded into postgres.") # confirmation message for logs

default_args = {
    'owner': 'airflow', # Owner name for UI reference
    'retries': 1, # number of retries on failure
    'retry_delay': timedelta(minutes=1), #wait time between tries
}

with DAG(
    dag_id='covid_api_etl', # Unique ID for the DAG
    default_args=default_args,
    description='ETL DAG for COVID-19 data using Airflow',
    schedule_interval='@daily',  # DAG runs once daily
    start_date=datetime(2025, 1, 1), # Start scheduling from this date
    catchup=False,
) as dag:
    # Task to extract COVID-19 data from public API
    extract_task = PythonOperator(
        task_id='extract_covid_data',
        python_callable=extract_covid_data
    ) # Task to clean and transform the extracted data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    ) # Task to load the transformed data into PostgreSQL database
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )
    # Define the order of task execution (ETL sequence)
    extract_task >> transform_task >> load_task #extract runs first, then transform, then load
