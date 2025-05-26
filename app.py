from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.linalg import Vectors

app = Flask(__name__)
spark = SparkSession.builder.appName("WeatherPredictionAPI").getOrCreate()
model = RandomForestRegressionModel.load("hdfs:///weather/models/rf_model")

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json(force=True)
    features = Vectors.dense([data['temperature'], data['humidity'], data['wind_speed']])
    df = spark.createDataFrame([(features,)], ["features"])
    prediction = model.transform(df).collect()[0]['prediction']
    return jsonify({'rainfall_prediction': prediction})

if __name__ == '__main__':
    app.run(debug=True)
