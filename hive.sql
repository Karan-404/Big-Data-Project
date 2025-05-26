CREATE EXTERNAL TABLE IF NOT EXISTS weather_data (
  station_id STRING,
  wmo_id STRING,
  datetime STRING,
  temperature FLOAT,
  humidity FLOAT,
  pressure FLOAT,
  alt_pressure FLOAT,
  wind_speed FLOAT,
  wind_dir FLOAT,
  max_temp FLOAT,
  min_temp FLOAT,
  rainfall FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/weather/data/';
