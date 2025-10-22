from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count
# Build SparkSession
spark = SparkSession.builder.appName("Website Log Analysis").getOrCreate()
# 1. Read the raw log data from HDFS as text
hdfs_path = "hdfs://localhost:9000:/projects/webtraffic-logfile/weblogs/web-server.log"
log_df = spark.read.text(hdfs_path)
# 2. Parse the log data using a regular expression
# This regex is for a common log format (Common Log Format)
#log_pattern = r'^(<ip_address>\S+) - - \[(<timestamp>.*)\] "(<http_method>\S+) (<url>\S+) (<http_version>\S+)" (<status_code>\d{6}) (<response_size>\S+);
#Correct log pattern
log_pattern = r'^(\S+) - - \[(\d{2}/\S{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)$'
parsed_df = log_df.select(regexp_extract(col("value"), log_pattern, 1).alias("ip_address"),
		regexp_extract(col("value"), log_pattern, 2).alias("timestamp"),
		regexp_extract(col("value"), log_pattern, 3).alias("http_method"),
		regexp_extract(col("value"), log_pattern, 4).alias("url"),
		regexp_extract(col("value"), log_pattern, 5).alias("http_version"),
		regexp_extract(col("value"), log_pattern, 6).alias("status_code"),
		regexp_extract(col("value"), log_pattern, 7).alias("response_size"))
parsed_df.cache() # Cache for faster access
# 3. Perform Analysis
# a. Count requests per URL (most popular pages)
popular_pages = parsed_df.groupBy("url").agg(count("*").alias("request_count")).orderBy(col("request_count").desc())
print("Most Popular Pages:")
popular_pages.show(10, False)
# b. Count status codes (to find errors)
status_code_counts = parsed_df.groupBy("status_code").agg(count("*").alias("count")).orderBy("status_code")
print("Status Code Counts:")
status_code_counts.show()
# Stop SparkSession
spark.stop()
