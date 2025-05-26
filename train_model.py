from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("WeatherPrediction").getOrCreate()

data = spark.read.csv("hdfs:///weather/preprocessed/", header=True, inferSchema=True)

assembler = VectorAssembler(inputCols=["temperature", "humidity", "wind_speed"], outputCol="features")
data = assembler.transform(data)

train_data, test_data = data.randomSplit([0.8, 0.2])

rf = RandomForestRegressor(featuresCol="features", labelCol="rainfall")
model = rf.fit(train_data)

predictions = model.transform(test_data)

evaluator = RegressionEvaluator(labelCol="rainfall", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

model.save("hdfs:///weather/models/rf_model")

spark.stop()
