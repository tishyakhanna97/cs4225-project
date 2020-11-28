import argparse
from google.cloud import language_v1
from pyspark.sql import SparkSession

app_name = "Analyze Sentiment"
master = 'local'

spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()

def analyze_tweet(tweet_text: str):
    client = language_v1.LanguageServiceClient()
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
    parser.add_argument(
        "tweets_file",
        help="The filename of tweets from a particular day.",
    )
    args = parser.parse_args()

    df = spark.read.text(args.tweets_file)
    df.map(lambda tweet: (tweet, analyze_tweet(tweet)))