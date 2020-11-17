#!/usr/bin/sh

export GOOGLE_APPLICATION_CREDENTIALS="PATH_TO_CREDENTIAL_FILE"

gcloud dataproc jobs submit pyspark jobs/regression.py \
    --cluster=tweeets-processing \
    --region=asia-southeast1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar