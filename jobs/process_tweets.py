#!/usr/bin/env python

import pyspark
import sys
from pyspark.sql import SQLContext

def extract_date(file_name):
  return file_name.split()[1]

def get_average_sentiments(sentiments_row):
  rows = sentiments_row.splitlines()
  total = 0
  for row in rows:
    sentiment = row.split(',')[1]
    total += float(sentiment)
  return total / len(rows)

if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

inputUri=sys.argv[1]
# outputUri=sys.argv[2]

sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)
rdd_csv = sc.wholeTextFiles(inputUri)
file_name_rdd = rdd_csv.map(lambda ele: (extract_date(ele[0]), ele[1]))
processed_rdd = file_name_rdd.map(lambda ele: (ele[0], get_average_sentiments(ele[1])))
for i in processed_rdd.collect():
  print('the date is ' + i[0])
  print(i[1])

df = sqlContext.createDataFrame(processed_rdd, ['date', 'sentiment'])
df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save(sys.argv[2])
