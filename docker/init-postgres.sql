-- Create the airflow database alongside the feature_store DB
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO featurestore;
