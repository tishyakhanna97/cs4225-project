import os
import sys
import shutil
import argparse
from google.cloud import language_v1
from pyspark.sql import SparkSession

app_name = "Analyze Sentiment"
master = 'local'

spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()

def analyze_tweet(tweet_text: str):
    client = language_v1.LanguageServiceClient.from_service_account_json("tweets_processing/sentiment_analysis/CREDENTIALS.json")
    tweet_document = language_v1.Document(content=tweet_text, type_=language_v1.Document.Type.PLAIN_TEXT)
    annotations = client.analyze_sentiment(request={'document': tweet_document})
    score = annotations.document_sentiment.score
    magnitude = annotations.document_sentiment.magnitude
    
    # # Per sentence scores
    # for sentence in annotations.sentences:
    #     sentence_sentiment = sentence.sentiment.score
    #     print(f"{sentence}{sentence_sentiment}")

    return score * magnitude

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    tweets_file = sys.argv[1]

    df = spark.read.text(tweets_file)
    tweets_with_scores = df.rdd.map(lambda tweet: (tweet.value, analyze_tweet(tweet.value)))
    output_path = os.path.join(os.path.dirname(tweets_file), "scores")
    
    if os.path.exists(output_path):
      shutil.rmtree(output_path)
    tweets_with_scores.saveAsTextFile(output_path)
