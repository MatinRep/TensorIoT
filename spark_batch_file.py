from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, to_date

# Creating a Spark session:
spark = SparkSession.builder.appName("MillionReviewsAnalysis").getOrCreate()

# Reading the million reviews file into deticated schemas: 
file_path = '100k_a.csv'
column_names = ["User_Id","Streaming_Id","Streamer_Username","Time_Start","Time_Stop"]
df = spark.read.csv(file_path, header=True, inferSchema=True, schema=column_names)

# Item having the least rating:
least_rated_item = df.orderBy("Streaming_Id").limit(1).collect()[0]

# Item having the most rating:
most_rated_item = df.orderBy(col("Streaming_Id").desc()).limit(1).collect()[0]

# Item having the longest reviews:
df = df.withColumn("result", col("Time_Stop") - col("Time_Start"))
df_sorted = df.orderBy(col("result").desc())


# Showing a desired data frame operation (e.g., group by and calculate average rating):
average_rating_by_category = df.groupBy("Streaming_Id").agg({"result": "avg"})

# Converting the whole file into Parquet file after transforming:

df_transformed.write.parquet(mode='overwrite')

# Spark session End:
spark.stop()
