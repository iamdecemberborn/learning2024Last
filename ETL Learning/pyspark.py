from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CachingExample").getOrCreate()

# Sample DataFrame
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
columns = ["Name", "ID"]
df_from_csv = spark.createDataFrame(data, columns)

# Cache the DataFrame
df_from_csv.cache()

# Perform an action to trigger caching
df_from_csv.show()
