from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

# --- Configuration ---
IMAGE_DIR = "file:///home/rajmadhekar/DataScience/projects/str-abualc/fake_images"
# Define a checkpoint location to store stream metadata (REQUIRED for structured streaming)
CHECKPOINT_DIR = "file:///tmp/spark_checkpoint_dir"
SCHEMA_PATH = "file:///tmp/spark_schema_images" # Temporary folder to store inferred schema

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("ImageFileStreamProcessor") \
    .getOrCreate()
    
# Set log level to WARN to minimize console output
spark.sparkContext.setLogLevel("WARN")

# 2. Define the Streaming Source
# Spark reads the IMAGE_DIR as a file stream.
# 'format("image")' tells Spark to load the files as binary image data.
image_stream_df = spark.readStream \
    .format("image") \
    .option("pathGlobFilter", "*.jpg") \
    .load(IMAGE_DIR)

# 3. Process the Stream Data
# The resulting DataFrame (image_stream_df) will have columns like:
# 'image.origin' (the file path), 'image.width', 'image.height', 'image.nChannels', 
# 'image.mode', and 'image.data' (the raw binary image data).

processed_df = image_stream_df.select(
    input_file_name().alias("file_path"), # Get the full path of the file
    current_timestamp().alias("processing_time"),
    "image.width",
    "image.height",
    "image.nChannels",
    "image.data" # The actual binary image content
)

# 4. Define the Streaming Sink (Output)
# For testing and demonstration, we use 'console' sink. 
# In a real-world scenario, you might write to Parquet, Kafka, or another system.
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') # Check for new files every 5 seconds
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

print("Spark Streaming is running. Monitoring directory for new images...")
print(f"File paths will be read from: {IMAGE_DIR}")

# Wait for the termination of the query
query.awaitTermination()
