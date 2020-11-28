from google.cloud import storage
import os
from multiprocessing import Pool
from glob import glob
from datetime import datetime
import dateparser

# Globals
CREDENTIALS_JSON = "/Users/sushinoya/Downloads/tweets/cs4225-294613-666c370bb34b.json"
BUCKET_NAME = "tweets-unclean"

# Upload Script
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_JSON


def rename_blob(blob_name, new_name, bucket_name=BUCKET_NAME):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_name)

    print("Blob {} has been renamed to {}".format(blob.name, new_blob.name))

def get_files_info():
  with open("dates.txt", "r") as files_with_dates:
    lines = files_with_dates.readlines()
    output = []

    for line in lines:
      name, rest = line.split(": ")
      date_range = rest[rest.find("(")+1: rest.find(")")]
      start_date, end_date = date_range.split(" - ")

      star_date_str = dateparser.parse(start_date).date().__str__()
      output.append((name, star_date_str))
    
    return output

if __name__ == '__main__':
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(BUCKET_NAME)

    info = get_files_info()

    for file_name, start_date in info:
      try:
        rename_blob(file_name, f"{file_name}: {start_date}")
      except Exception as e:
        print(e)

