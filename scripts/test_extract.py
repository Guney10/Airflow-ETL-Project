import pandas as pd
import requests

url = "https://disease.sh/v3/covid-19/countries"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)
    print(df.head())
else:
    print("Failed to fetch data:", response.status_code)