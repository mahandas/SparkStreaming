# Databricks notebook source
#Import Libaries
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkConf, SparkContext
import itertools
import sys
from operator import add
from math import sqrt

# COMMAND ----------


def RMSEcomputation(model, data, n):
    #Compute RMSE (Root Mean Squared Error).
    predicts = model.predictAll(data.map(lambda x: (x[0], x[1])))
    rateAndPredict = predicts.map(lambda x: ((x[0], x[1]), x[2])).join(data.map(lambda x: ((x[0], x[1]), x[2]))).values()
    return sqrt(rateAndPredict.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

# COMMAND ----------

if __name__ == "__main__":
    conf = SparkConf().setAppName("MovieLensALS").set("spark.executor.memory", "2g")
#     sc = SparkContext(conf=conf)

    # Load and parse the data
    data = sc.textFile("/FileStore/tables/ratings.dat")
    rates = data.map(lambda l: l.strip().split('::')).map(
        lambda l: (float(l[3]) % 10, (int(l[0]), int(l[1]), float(l[2]))))


    numPartitions = 4
    (training,validation, test) = rates.randomSplit([0.65,0.05,0.3])
    training = training.values().repartition(numPartitions).cache()
    validation = validation.values().repartition(numPartitions).cache()
    test = test.values().cache()

    trainNum = training.count()
    validateNum = validation.count()
    testNum = test.count()
    print("Training: %d, validation: %d, test: %d" %
        (trainNum, validateNum, testNum))

    ranks = [4, 8]
    lambdas = [0.1, 10.0]
    numIters = [5, 10]
    modelBest = None
    bestValRmse = float("inf")
    rankBest = 0
    lambdaBest = -1.0
    numCountBest = -1
    print("sfsfsf")
    for rank, lambdaVal, numIter in itertools.product(ranks, lambdas, numIters):
        model = ALS.train(training, rank, numIter, lambdaVal)
        validationRmse = RMSEcomputation(model, validation, validateNum)
        print("RMSE (validation) = %f for the model trained with " % validationRmse +
            "rank = %d, lambda = %.1f, and numIter = %d." % (
                rank, lambdaVal, numIter))
        if (validationRmse < bestValRmse):
            modelBest = model
            bestValRmse = validationRmse
            rankBest = rank
            lambdaBest = lambdaVal
            numCountBest = numIter

        testRmse = RMSEcomputation(modelBest, test, testNum)

        print("The best model was trained with rank -> %d and lambda -> %.1f, " % (rankBest, lambdaBest)
            + "and with Iterations -> %d, the RMSE for the test set is %f." % (numCountBest, testRmse))

# COMMAND ----------

testRmse

# COMMAND ----------

rates
