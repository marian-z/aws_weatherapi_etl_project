# Weather Data ETL Pipeline

# Project Overview

This project is designed to extract weather data from the OpenWeatherMap API and store it in an AWS S3 bucket. Using an EC2 instance for computation, the solution leverages AWS Secrets Manager to securely manage credentials and access keys. The pipeline is orchestrated using Apache Airflow, ensuring reliable data ingestion and transformation.

# Key Components
1. OpenWeatherMap API: Extracts current weather data for Prague.
2. AWS EC2: Hosts the Airflow environment for executing the ETL process.
3. AWS S3: Serves as the storage solution for processed weather data.
4. AWS Secrets Manager: Stores sensitive information like access keys, which are referenced in the code, enhancing security.
5. IAM Role: Grants the EC2 instance permissions (AmazonEC2FullAccess, AmazonS3FullAccess, SecretsManagerReadWrite) to perform necessary operations without hardcoding credentials.
   
# Setup Instructions
1. Launch an EC2 instance.
2. Create an S3 bucket for storage.
3. Store access keys in AWS Secrets Manager.
4. Set up an IAM role with appropriate permissions for the EC2 instance.
5. Create a virtual environment and install dependencies:

   sudo apt update
   sudo apt install python3-pip
   sudo apt install python3.10-venv
   python3 -m venv airflow_venv
   pip install pandas s3fs apache-airflow awscli boto3

# Code Implementation
The main logic resides in weather_dag.py, where:
DAG Definition: 
Configures tasks for checking API availability, extracting weather data, and transforming/loading the data into S3.
Data Transformation: 
Converts temperature from Kelvin to Celsius and structures the data into a DataFrame for storage.

For further details, please refer to the code in the weather_dag.py file.
