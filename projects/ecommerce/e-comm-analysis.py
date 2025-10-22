from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum
# Build SparkSession
spark = SparkSession.builder.appName("E-commerce Analysis").config("spark.jars", "/user/home/rajmadhekar/mysql-connector-j-9.4.0/mysql-connector-j-9.4.0.jar").getOrCreate()
# 1. Read the large transaction data from HDFS
hdfs_path = "hdfs://localhost:9000/projects/ecommerce/e-comm-trax.csv"
transactions_df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
transactions_df.cache() # Cache for faster access
# 2. Read the smaller, structured data from MySQL
jdbc_url = "jdbc:mysql://localhost:3306/e_commerce_db"
db_properties = {"user": "rajmadhekar", # Replace with your MySQL user
    "password": "R@Jr4tN4", # Replace with your MySQL password
    "driver": "com.mysql.cj.jdbc.Driver"}
products_df = spark.read.jdbc(url=jdbc_url, table="products", properties=db_properties)
# 3. Join the DataFrames
joined_df = transactions_df.join(products_df, on="product_id", how="left")
# 4. Perform Analysis
# a. Total sales per product category
sales_by_category = joined_df.groupBy("product_category").agg(sum("transaction_amount").alias("total_sales")).orderBy(col("total_sales").desc())
print("Total Sales by Product Category:")
sales_by_category.show()
# b. Monthly sales trend
monthly_sales = joined_df.withColumn("year", year(col("purchase_date"))).withColumn("month", month(col("purchase_date"))).groupBy("year", "month").agg(sum("transaction_amount").alias("monthly_sales")).orderBy("year", "month")
print("Monthly Sales Trend:")
monthly_sales.show()
# Stop SparkSession
spark.stop()
