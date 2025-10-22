from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType
from textblob import TextBlob
# Build SparkSession
spark = sparkSession.builder.appName("Social-Media Sentiment Analysis").getOrCreate()
# 1. Read the raw data from HDFS
hdfs_path = "hdfs://localhost:9000/projects/social-media/social-media-data.csv"
tweets_df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
tweets_df.cache()
# 2. Define a sentiment analysis function
def get_sentiment(text):
	"""
	Analyzes the sentiment of a given text using TextBlob.
	"""
	if text is None:
		return "neutral"

	analysis = TextBlob(text)

	if analysis.sentiment.polarity > 0:
		return "positive"
	elif analysis.sentiment.polarity < 0:
		return "negative"
	else:
		return "neutral"
# 3. Register the function as a PySpark UDF
get_sentiment_udf = udf(get_sentiment, StringType())
# 4. Apply the UDF to the DataFrame
sentiment_df = tweets_df.withColumn("sentiment",get_sentiment_udf(col("tweet_text")))
# 5. Perform Analysis (aggregation)
sentiment_by_topic = sentiment_df.groupBy("topic", "sentiment").count().orderBy("topic", col("count").desc())
print("Sentiment by Topic:")
sentiment_by_topic.show()
# Stop SparkSession
spark.stop()
