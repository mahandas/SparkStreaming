# Databricks notebook source
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

lines = spark.read.text("dbfs:/FileStore/shared_uploads/mahdas@ebay.com/ratings.dat").rdd
parts = lines.map(lambda row: row.value.split("::"))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
                                     rating=float(p[2]), timestamp=int(p[3])))
ratings = spark.createDataFrame(ratingsRDD)
(training, test) = ratings.randomSplit([0.7, 0.2])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# COMMAND ----------

dbfs:/FileStore/shared_uploads/mahdas@ebay.com/movies.dat
dbfs:/FileStore/shared_uploads/mahdas@ebay.com/itemusermat

# COMMAND ----------

from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import pandas as pd
data = sc.textFile("dbfs:/FileStore/shared_uploads/mahdas@ebay.com/itemusermat")
businessDF = spark.read \
    .option("sep", "::") \
    .csv("dbfs:/FileStore/shared_uploads/mahdas@ebay.com/movies.dat") \
    .withColumnRenamed("_c0", "movie_id_") \
    .withColumnRenamed("_c1", "title") \
    .withColumnRenamed("_c2", "genre") \
    .dropDuplicates()
parsedData = data.map(lambda line: array([int(x) for x in line.split(' ')[1:]]))
clusters = KMeans.train(parsedData, 10, maxIterations=10, initializationMode="random")

def getcluster(point):
    center = clusters.predict(point[1:])
    return (point[0], center)

WSSSE = data.map(lambda line: array([int(x) for x in line.split(' ')])).map(lambda point: getcluster(point)).collect()
deptColumns = ["movie_id","cluster"]
te = pd.DataFrame(list(WSSSE), columns = deptColumns)
deptDF = spark.createDataFrame(te)

df = deptDF.join(businessDF, businessDF.movie_id_ == deptDF.movie_id, how="inner")

window = Window.partitionBy(df['cluster']).orderBy(df['movie_id'].desc())
  
df.select('*', rank().over(window).alias('rank'))\
  .filter(col('rank') <= 5)\
  .show(100)

# COMMAND ----------

from pyspark.ml.recommendation import ALS

# Let's initialize our ALS learner
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")

# Now let's compute an evaluation metric for our test dataset
from pyspark.ml.evaluation import RegressionEvaluator

# Create an RMSE evaluator using the label and predicted columns
reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="rmse")

tolerance = 0.03
ranks = [4, 8, 12]
errors = [0, 0, 0]
models = [0, 0, 0]
err = 0
min_error = float('inf')
best_rank = -1
for rank in ranks:
  # Create the model with these parameters.
  model = als.fit(training)
  # Run the model to create a prediction. Predict against the validation_df.
  predict_df = model.transform(test)

  # Remove NaN values from prediction (due to SPARK-14489)
  predicted_ratings_df = predict_df.filter(predict_df.prediction != float('nan'))

  # Run the previously created RMSE evaluator, reg_eval, on the predicted_ratings_df DataFrame
  error = reg_eval.evaluate(predicted_ratings_df)
  errors[err] = error
  models[err] = model
  print ('For rank %s the RMSE is %s' % (rank, error))
  if error < min_error:
    min_error = error
    best_rank = err
  err += 1

als.setRank(ranks[best_rank])
print('The best model was trained with rank %s' % ranks[best_rank])
my_model = models[best_rank]
