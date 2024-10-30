# Import necessary libraries
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
import os
from botocore.exceptions import ClientError

# Function to retrieve AWS secrets from AWS Secrets Manager
def get_aws_secrets(secret_name):
    # Create a session with AWS
    session = boto3.session.Session()
    
    # Initialize the Secrets Manager client
    client = session.client(
        service_name='secretsmanager',
        region_name='us-east-1'
    )

    try:
        # Retrieve the secret value using the secret name
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # Raise an error if the secret cannot be retrieved
        raise e

    # Parse the secret string into a JSON object
    secret = json.loads(response['SecretString'])
    return secret

# Fetch AWS credentials stored in Secrets Manager for later use
aws_secrets = get_aws_secrets('weather_api_etl_project')

# Function to convert temperature from Kelvin to Celsius
def kelvin_to_celsius(temp_in_kelvin):
    # Convert Kelvin to Celsius using the formula
    temp_in_celsius = temp_in_kelvin - 273.15
    return round(temp_in_celsius, 2)

# Function to transform and load data into S3
def transform_load_data(task_instance):
    # Pull data from the previous task in the DAG
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    
    # Extract relevant fields from the JSON response
    city = data["name"]  # City name
    weather_description = data["weather"][0]['description']  # Weather description
    temp_celsius = kelvin_to_celsius(data["main"]["temp"])  # Current temperature
    feels_like_celsius = kelvin_to_celsius(data["main"]["feels_like"])  # Feels like temperature
    min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])  # Minimum temperature
    max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])  # Maximum temperature
    pressure = data["main"]["pressure"]  # Atmospheric pressure
    humidity = data["main"]["humidity"]  # Humidity percentage
    wind_speed = data["wind"]["speed"]  # Wind speed
    # Convert timestamps to UTC datetime objects
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    # Create a dictionary with transformed data
    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (C)": temp_celsius,
        "Feels Like (C)": feels_like_celsius,
        "Minimun Temp (C)": min_temp_celsius,
        "Maximum Temp (C)": max_temp_celsius,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time                        
    }

    # Convert the dictionary into a DataFrame for easier manipulation and storage
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    # Prepare AWS credentials for accessing S3
    aws_credentials = {
        "key": aws_secrets["access_key_id"],
        "secret": aws_secrets["secret_access_key"],
    }

    # Generate a timestamped filename for the CSV file
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")  # Format the date and time
    dt_string = 'current_weather_data_prague_' + dt_string  # Create a unique filename

    # Save the DataFrame as a CSV file in the specified S3 bucket
    df_data.to_csv(f"s3://weatherapiairflowprojectmz/{dt_string}.csv", index=False, storage_options=aws_credentials)

# Default arguments for the Airflow DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Do not depend on past DAG runs
    'start_date': datetime(2024, 1, 1),  # Start date for scheduling
    'email': ['myemail@domain.com'],  # Email for notifications
    'email_on_failure': False,  # Do not send email on failure
    'email_on_retry': False,  # Do not send email on retry
    'retries': 0,  # Number of retries for tasks
    'retry_delay': timedelta(minutes=2)  # Delay between retries
}

# Define the Weather DAG
with DAG('weather_dag',
        default_args=default_args,
        description='DAG to ingest, process and load data from OpenWeatherMap API to S3 storage',
        schedule_interval='@daily',  # Schedule to run daily
        catchup=False) as dag:

    # Extract the weather API key from the secrets for authentication
    weather_api_key = aws_secrets["weather_api_key"]        

    # Sensor task to check if the weather API is available
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint=f'/data/2.5/weather?q=Prague&APPID={weather_api_key}'  # API endpoint to check availability
    )

    # Task to extract weather data from the API
    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint=f'/data/2.5/weather?q=Prague&APPID={weather_api_key}',  # API endpoint for data extraction
        method='GET',  # HTTP method
        response_filter=lambda r: json.loads(r.text),  # Filter to parse JSON response
        log_response=True  # Enable logging of the response
    )

    # Task to transform and load the weather data into S3
    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',  # Task ID
        python_callable=transform_load_data  # Reference to the transformation function
    )

    # Define the task dependencies
    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data  # Execute in order
