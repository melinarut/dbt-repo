import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, types
from sqlalchemy.dialects.postgresql import JSON as postgres_json
from sqlalchemy_utils import database_exists, create_database
import os
import requests
import datetime
import json
from dotenv import dotenv_values

config = dotenv_values("C:\\Users\\melin\\OneDrive\\Dokumente\\Spiced_Academy_Bootcamp\\dbt-bootcamp\\07_data_engineer\\notebook\\token.env")

username = config['POSTGRES_USER']
password = config['POSTGRES_PW']
host = config['POSTGRES_HOST']
port = config['POSTGRES_PORT']
db_climate = config['DB_CLIMATE']

url = f'postgresql://{username}:{password}@{host}:{port}/{db_climate}'

engine = create_engine(url, echo=True)

locations = ['Shanghai','Manila','Perth','Singapore']

weather_api_key = config['weatherapi']

weather_dict = {'extracted_at':[], 'extracted_data':[]}

for city in locations:
     for day in pd.date_range(start='06/20/2023', end='06/22/2023'):
         requested_day = day.date()
         print(city, requested_day)
         api_url = f'http://api.weatherapi.com/v1/history.json?key={weather_api_key}&q={city}&dt={requested_day}'
         response = requests.request("GET", api_url)
         if response.status_code == 200:
            print(f'attempt for {day.date()} in {city} resulted in {response.status_code}', end='\r')
            dt = datetime.datetime.now() 
            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S") 
            weather_dict['extracted_at'].append(dt_str)
            weather_dict['extracted_data'].append(json.loads(response.text))
         else:
            print(f'for date: {day.date()} and city: {city} status code {response.status_code} -> research error')


json_data = json.dumps(weather_dict) 

with open('weather_dict.json',mode='w') as f:
    f.write(json.dumps(weather_dict))

weather_dict_df = pd.DataFrame(weather_dict)

dtype_dict = {'extracted_at':types.DateTime, 'extracted_data':postgres_json}

weather_dict_df.to_sql('weather_raw', engine, if_exists='replace', dtype=dtype_dict)
