from google.cloud import storage
import os
from multiprocessing import Pool
from glob import glob

# Globals
CREDENTIALS_JSON = "/Users/sushinoya/Downloads/tweets/cs4225-293503-b4ff44e6c310.json"
BUCKET_NAME = "twitter-data-autoupload"
LAST_UPLOADED_INDEX = 10

# Upload Script
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_JSON

def upload_blob(source_file_name, destination_blob_name=None, bucket_name=BUCKET_NAME):	
	if not destination_blob_name:
		destination_blob_name = source_file_name

	storage_client = storage.Client()
	bucket = storage_client.bucket(bucket_name)
	blob = bucket.blob(destination_blob_name)

	blob.chunk_size = 5 * 1024 * 1024  # Set 5 MB blob size to prevent timeout

	try:
		blob.upload_from_filename(source_file_name)
	except Exception as e:
		print("Error occured, retrying...")
		upload_blob(source_file_name)


if __name__ == '__main__':
	files = sorted(glob("uncompressed/*.csv"))  # ["corona_tweets_01.csv", "corona_tweets_02.csv", "corona_tweets_03.csv"]
	pool = Pool(processes=16)
	r = pool.map_async(upload_blob, files)
	r.wait()
	# for i, file in enumerate(files):
	# 	index = i + 1
	# 	if index <= LAST_UPLOADED_INDEX:
	# 		continue
		
	# 	print(f"{index}. STARTED UPLOAD: {file}")
	# 	upload_blob(source_file_name=file)
	# 	print(f"{index}. FINISHED UPLOAD: {file}")

	# try:
	# 	upload_blob(i + 1, source_file_name=file)
	# except Exception as e:
	# 	print("Error occured, retrying...")
