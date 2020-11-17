"""Run a regression using Apache Spark ML.

In the following PySpark (Spark Python API) code, we take the following actions:

  * Load a previously created  regression (BigQuery) input table
    into our Cloud Dataproc Spark cluster as an RDD (Resilient
    Distributed Dataset)
  * Transform the RDD into a Spark Dataframe
  * Vectorize the features on which the model will be trained
  * Compute a  regression using Spark ML

"""

from pyspark.context import SparkContext
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.sql.session import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler

def create_train_test_df(dependent_variable, data_vector):
    data_vector = data_vector.select(['features', dependent_variable])
    return data_vector.randomSplit([0.7, 0.3])


def run_GBT_Regression(dependent_variable, data_vector):
    train_df, test_df = create_train_test_df(dependent_variable, data_vector)

    regressor = GBTRegressor(featuresCol='features', labelCol=dependent_variable, maxIter=10)
    regressor_model = regressor.fit(train_df)
    print('feature importance is {}'.format(regressor_model.featureImportances))

    # Make predictions.
    predictions = regressor_model.transform(test_df)

    # Select example rows to display.
    predictions.select("prediction", dependent_variable, "features").show()

    # Select (prediction, true label) and compute test error
    r2_evaluator = RegressionEvaluator(
        labelCol=dependent_variable, predictionCol="prediction", metricName="r2")
    rmse_evaluator = RegressionEvaluator(
        labelCol=dependent_variable, predictionCol="prediction", metricName="rmse")

    print("R Squared (R2) on test data = %g" % r2_evaluator.evaluate(predictions))
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse_evaluator.evaluate(predictions))

    return [r2_evaluator.evaluate(predictions), rmse_evaluator.evaluate(predictions), regressor_model.featureImportances.toArray().tolist()]

def run_linear_regression(dependent_variable, data_vector):
    train_df, test_df = create_train_test_df(dependent_variable, data_vector)
    regressor = LinearRegression(featuresCol='features', labelCol=dependent_variable, maxIter=10, regParam=0.3, elasticNetParam=0.8)

    ########### TRAINING DATA ##########
    regressor_model = regressor.fit(train_df)
    print("Coefficients: " + str(regressor_model.coefficients))
    print("Intercept: " + str(regressor_model.intercept))

    trainingSummary = regressor_model.summary
    training_rmse = trainingSummary.rootMeanSquaredError
    training_r2 = trainingSummary.r2
    print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    print("r2: %f" % trainingSummary.r2)


    # Make predictions.
    predictions = regressor_model.transform(test_df)

    # Select example rows to display.
    predictions.select("prediction", dependent_variable, "features").show()

    # Select (prediction, true label) and compute test error
    r2_evaluator = RegressionEvaluator(
        labelCol=dependent_variable, predictionCol="prediction", metricName="r2")
    print("R Squared (R2) on test data = %g" % r2_evaluator.evaluate(predictions))

    rmse_evaluator = RegressionEvaluator(
        labelCol=dependent_variable, predictionCol="prediction", metricName="rmse")
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse_evaluator.evaluate(predictions))

    return [training_r2, training_rmse, r2_evaluator.evaluate(predictions), rmse_evaluator.evaluate(predictions)]

def average_column(column_number, all_results):
    wanted = [x[column_number] for x in all_results]
    if isinstance(wanted[0], list): #return result shld be a list
        size = len(wanted[0])
        return [average_column(col, wanted) for col in range(size)]
    elif isinstance(wanted[0], (int, float, complex)):
        return sum(wanted) / len(wanted)

sc = SparkContext()
spark = SparkSession(sc)
dependent_variable = 'high'

#TODO: change the variable below
mode = 'L' # L for linear regression, G for GBT

# Read the data from BigQuery as a Spark Dataframe.
data = spark.read.format("bigquery").option(
    "table", "cs4225-294613:joined_data.date-new_confirmed-high-trends-sentiment").load()
# Create a view so that Spark SQL queries can be run against the data.
data.createOrReplaceTempView("data")

# As a precaution, run a query in Spark SQL to ensure no NULL values exist.
sql_query = """
SELECT *
from data
where num_cases is not null
and Coronavirus___Worldwide_ is not null
and sentiment is not null
"""
clean_data = spark.sql(sql_query)

vectorAssembler = VectorAssembler(inputCols = ['num_cases', 'Coronavirus___Worldwide_', 'sentiment'], outputCol = 'features')
data_vector = vectorAssembler.transform(clean_data)

all_results = []
for i in range(10):
    if mode == 'G':
        result = run_GBT_Regression(dependent_variable, data_vector)
        all_results.append(result)
    else:
        result = run_linear_regression(dependent_variable, data_vector)
        all_results.append(result)

# average results over all the runs
num_cols = len(all_results[0])
result = [average_column(col, all_results) for col in range(num_cols)]
print('final result is')
print(result)

