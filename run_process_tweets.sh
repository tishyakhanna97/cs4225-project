#!/usr/bin/sh

export GOOGLE_APPLICATION_CREDENTIALS="PATH_TO_CREDENTIAL_FILE"

gcloud dataproc jobs submit pyspark jobs/process_tweets.py \
    --cluster=tweeets-processing \
    --region=asia-southeast1 \
    -- gs://tweets-unclean/ gs://tweets-test-bucket/output.csv/
