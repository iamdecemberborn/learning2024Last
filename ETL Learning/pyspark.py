

# from pyspark.sql import SparkSession
# # Initialize SparkSession
# spark = SparkSession.builder.appName("MultipleLazyEvaluationsExample").getOrCreate()
# # Sample DataFrame
# data = [("Alice", 1, 25), ("Bob", 2, 30), ("Cathy", 3, 28), ("David", 4, 35)]
# columns = ["Name", "ID", "Age"]
# df = spark.createDataFrame(data, columns)
# # Define multiple transformations (lazy evaluations)
# df_filtered = df.filter(df["ID"] > 1)  # First lazy evaluation
# df_selected = df_filtered.select("Name", "Age")  # Second lazy evaluation
# df_renamed = df_selected.withColumnRenamed("Name", "Employee_Name")  # Third lazy evaluation
# # No computation has occurred yet
# print("Multiple transformations defined, but not executed.")
# # Trigger actions for the first two transformations
# df_filtered.show()  # Triggering the first transformation
# df_selected.show()  # Triggering the second transformation
# # The third transformation remains unevaluated
# print("The third transformation has not been executed.")





# Import PySpark
import pyspark
from pyspark.sql import SparkSession
# Initialize Spark Session
spark = SparkSession.builder.appName("BroadcastExample").getOrCreate()
# Define a dictionary of state abbreviations and full names
states = {"NY": "New York", "CA": "California", "FL": "Florida"}
# Broadcast the dictionary
broadcastStates = spark.sparkContext.broadcast(states)
# Sample Data
data = [
    ("James", "Smith", "USA", "CA"),
    ("Michael", "Rose", "USA", "NY"),
    ("Robert", "Williams", "USA", "CA"),
    ("Maria", "Jones", "USA", "FL"),
]
# Convert data to RDD
rdd = spark.sparkContext.parallelize(data)
# Define a function to convert state codes to full state names
def state_convert(code):
    return broadcastStates.value[code]
# Transform the RDD: Replace state codes with full names
result = rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).collect()
# Print the output
print("Transformed Output:")
for row in result:
    print(row)
# Stop the Spark Session
spark.stop()


