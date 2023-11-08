from airflow import DAG
from datetime import timedelta,datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
import s3fs


with open("dags//credentials.txt","r") as f:
	data = f.readlines()
	accesskey = data[0][:-1]
	secretkey = data[1][:-1]

def kelvin_to_fahrenheit(temp):
    f = round(((temp-273.15)*(9/5))+32,2)
    return f



def transform_load_data(task_instance):
        data = task_instance.xcom_pull(task_ids = "extract_weather_data")
        city = data['name']
        weather_description = data['weather'][0]['description']
        temp_farenheit = kelvin_to_fahrenheit(data['main']['temp'])
        feels_like_temp =  kelvin_to_fahrenheit(data['main']['feels_like'])
        min_temp =  kelvin_to_fahrenheit(data['main']['temp_min'])
        max_temp =  kelvin_to_fahrenheit(data['main']['temp_max'])
        pressure = data['main']['pressure']
        humidity = data['main']['humidity']
        wind_speed = data['wind']['speed']
        date_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone']).date().strftime('%d/%m/%y')
        time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone']).time().strftime('%H:%M:%S')
        sunrise_time = datetime.utcfromtimestamp(data['timezone'] + data['sys']['sunrise']).time().strftime('%H:%M:%S')
        sunset_time = datetime.utcfromtimestamp(data['timezone'] + data['sys']['sunset']).time().strftime('%H:%M:%S')


        transformed_data = {
                "City" : city,
                "Description" : weather_description,
                "Temperature (F)" : temp_farenheit,
                "Feels Like (F)" : feels_like_temp,
                "Max temp (F)" : max_temp,
                "Min temp (F)" : min_temp,
                "Pressure" : pressure,
                "Humidity" : humidity,
                "Wind Speed" : wind_speed,
                "Time of Record" : time_of_record,
                "Sunrise (LocalTime)" : sunrise_time,
                "Sunset (LocalTime)" : sunset_time
        }

        transformed_data_list = [transformed_data]
        df_data = pd.DataFrame(transformed_data_list)
        aws_credentials = {
               "key" : accesskey,
               "secret" : secretkey

        }

        now = datetime.now()
        dt_string = now.strftime('%d%m%y'+'-'+'%H%M%S')
        dt_string = 'CurrentWeatherData-Pune-'+ dt_string
        df_data.to_csv(f"s3://airflow-openweather-etl-maaz/{dt_string}.csv", index = False, storage_options = aws_credentials)





default_args = {
    'owner' : 'airflow',      
    'depends_on_past' : False,
    'start_date' : datetime(2023,11,1),
    'email' : ['tajmohammedmaaz@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=2)
}



with DAG('openweatherapi_dag',      #should be unique across airflow
         default_args = default_args,
         schedule_interval= '@daily',
         catchup = False) as dag:


         is_weather_api_ready = HttpSensor(
                 task_id = 'is_weather_api_ready', # Shuld be unique across a dag i.e. unique names shoild be used inside a dag
                 http_conn_id = 'weathermap_api',
                 endpoint = 'data/2.5/weather?q=Pune&appid=a4291d7d12349e2eade8b48a480c7f11'
         )


         extract_weather_data = SimpleHttpOperator(
                 task_id = 'extract_weather_data',
                 http_conn_id = 'weathermap_api',
                 endpoint = 'data/2.5/weather?q=Pune&appid=a4291d7d12349e2eade8b48a480c7f11',
                 method = 'GET',
                 response_filter = lambda r : json.loads(r.text),
                 log_response = True

         )

         transform_weather_data = PythonOperator(
                 task_id = 'transform_weather_data',
                 python_callable = transform_load_data
         )


         is_weather_api_ready >> extract_weather_data >> transform_weather_data
