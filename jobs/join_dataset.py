"""Create a Google BigQuery linear regression input table.

In the code below, the following actions are taken:
* A new dataset is created "natality_regression."
* A query is run against the public dataset,
    bigquery-public-data.samples.natality, selecting only the data of
    interest to the regression, the output of which is stored in a new
    "regression_input" table.
* The output table is moved over the wire to the user's default project via
    the built-in BigQuery Connector for Spark that bridges BigQuery and
    Cloud Dataproc.
"""

from google.cloud import bigquery

# Create a new Google BigQuery client using Google Cloud Platform project
# defaults.
client = bigquery.Client()

# Prepare a reference to a new dataset for storing the query results.
dataset_id = "new_ds"

dataset = bigquery.Dataset(client.dataset(dataset_id))

# Create the new BigQuery dataset.
dataset = client.create_dataset(dataset)

# In the new BigQuery dataset, create a reference to a new table for
# storing the query results.
table_ref = dataset.table("date-new_confirmed-high-trends-sentiment-finance")

# Configure the query job.
job_config = bigquery.QueryJobConfig()

# Set the destination table to the table reference created above.
job_config.destination = table_ref

# Set up a query in Standard SQL, which is the default for the BigQuery
# Python client library.
# The query selects the fields of interest.
query = """
SELECT cases.date,SUM(cases.new_confirmed) AS num_cases,stock.high,stock.low,stock.open,stock.close,trend.Coronavirus___Worldwide_,sentiments.sentiment,withdrawals.total_injection
FROM `cs4225-294613.cs4225.covid_data` cases
INNER JOIN `cs4225-294613.cs4225.stocks`stock ON cases.date = stock.Date
INNER JOIN `cs4225-294613.cs4225.trends`trend ON trend.Day = cases.date
INNER JOIN `cs4225-294613.cs4225.sentiment` sentiments ON sentiments.date = trend.Day
LEFT OUTER JOIN (SELECT Record_Date, SUM(Transactions_Today) AS total_injection 
FROM `cs4225-294613.cs4225.us_treasury`
WHERE Transaction_Type = "Withdrawals"
GROUP BY Record_Date) withdrawals ON withdrawals.Record_date = sentiments.date
GROUP BY cases.date,stock.high,stock.low,stock.open,stock.close,trend.Coronavirus___Worldwide_,sentiments.sentiment,withdrawals.total_injection
ORDER BY cases.date
"""

# Run the query.
query_job = client.query(query, job_config=job_config)
query_job.result()  # Waits for the query to finish