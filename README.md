# Weather Prediction Using Hadoop and Spark

## Overview

This project builds a weather prediction system by:

- Parsing and cleaning raw weather text data.
- Storing and querying data with Hive/HDFS.
- Preprocessing data with Hadoop MapReduce.
- Training a Random Forest model using Spark MLlib.
- Serving predictions via a Flask REST API.
- Containerizing the API using Docker.

## How to Run

### 1. Parse raw data:
python3 parse_weather_txt.py

### 2. Upload cleaned CSV to HDFS:
hdfs dfs -mkdir -p /weather/data/

hdfs dfs -put weather_cleaned.csv /weather/data/

### 3. Create Hive table:
Run the hive_create_table.sql script in Hive shell.

### 4. Run Hadoop MapReduce preprocessing:
hadoop jar WeatherPreprocessing.jar WeatherPreprocessing /weather/data/weather_cleaned.csv /weather/preprocessed/

### 5. Train model:
spark-submit train_weather_model.py

### 6. Run Flask API
python3 app.py
