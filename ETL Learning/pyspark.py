

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




#
# # Import PySpark
# import pyspark
# from pyspark.sql import SparkSession
# # Initialize Spark Session
# spark = SparkSession.builder.appName("BroadcastExample").getOrCreate()
# # Define a dictionary of state abbreviations and full names
# states = {"NY": "New York", "CA": "California", "FL": "Florida"}
# # Broadcast the dictionary
# broadcastStates = spark.sparkContext.broadcast(states)
# # Sample Data
# data = [
#     ("James", "Smith", "USA", "CA"),
#     ("Michael", "Rose", "USA", "NY"),
#     ("Robert", "Williams", "USA", "CA"),
#     ("Maria", "Jones", "USA", "FL"),
# ]
# # Convert data to RDD
# rdd = spark.sparkContext.parallelize(data)
# # Define a function to convert state codes to full state names
# def state_convert(code):
#     return broadcastStates.value[code]
# # Transform the RDD: Replace state codes with full names
# result = rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).collect()
# # Print the output
# print("Transformed Output:")
# for row in result:
#     print(row)
# # Stop the Spark Session
# spark.stop()
#



#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit
# # Step 1: Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("Merge Files with Different Schemas") \
#     .getOrCreate()
# # Step 2: Define file paths (replace with actual paths to your files)
# file1_path = "path_to_file1.csv"  # File1 with columns: id, name
# file2_path = "path_to_file2.csv"  # File2 with columns: id, age
# # Step 3: Read File1 and File2 into DataFrames
# df1 = spark.read.csv(file1_path, header=True, inferSchema=True)  # File1: id, name
# df2 = spark.read.csv(file2_path, header=True, inferSchema=True)  # File2: id, age
# # Display the original schemas
# print("Original File1 Schema:")
# df1.printSchema()
# print("Original File2 Schema:")
# df2.printSchema()
# # Step 4: Standardize Schemas by Adding Missing Columns
# # Add 'age' column to df1 with null values
# df1_standardized = df1.withColumn("age", lit(None))
# # Add 'name' column to df2 with null values
# df2_standardized = df2.withColumn("name", lit(None))
# # Display the standardized schemas
# print("Standardized File1 Schema:")
# df1_standardized.printSchema()
# print("Standardized File2 Schema:")
# df2_standardized.printSchema()
# # Step 5: Merge the DataFrames using unionByName()
# merged_df = df1_standardized.unionByName(df2_standardized)
# # Step 6: Show the Merged DataFrame
# print("Merged DataFrame:")
# merged_df.show()
# # Step 7: Save the Merged DataFrame to a CSV (optional)
# output_path = "path_to_output/merged_data.csv"
# merged_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
# # Stop the Spark Session
# spark.stop()






from pyspark.sql import SparkSession
# Step 1: Initialize SparkSession
spark = SparkSession.builder.appName("HandleBadData").getOrCreate()
# Step 2: Read CSV File with Mode Option
file_path = "path_to_corrupt_file.csv"  # Replace with actual file path
# Read file with PERMISSIVE mode (default), bad records will be null
df_permissive = spark.read.csv(
    file_path,
    header=True,
    mode="PERMISSIVE",  # Default mode, nulls for corrupt records
    inferSchema=True
)
print("DataFrame in PERMISSIVE mode:")
df_permissive.show(truncate=False)
# Read file with DROPMALFORMED mode, bad records will be dropped
df_dropmalformed = spark.read.csv(
    file_path,
    header=True,
    mode="DROPMALFORMED",  # Drop corrupt records
    inferSchema=True
)
print("DataFrame in DROPMALFORMED mode:")
df_dropmalformed.show(truncate=False)
# Step 3: Log Bad Records Separately
bad_records_path = "path_to_log_bad_records"
df_logged = spark.read.option("badRecordsPath", bad_records_path) \
    .csv(file_path, header=True, inferSchema=True)
print("DataFrame with badRecordsPath logged:")
df_logged.show(truncate=False)
# Step 4: Filter Out Invalid Rows Explicitly
# For example, removing rows where Emp_no is not numeric
df_filtered = df_permissive.filter("Emp_no IS NOT NULL AND Emp_no RLIKE '^[0-9]+$'")
print("Filtered DataFrame with valid records:")
df_filtered.show(truncate=False)
# Stop SparkSession
spark.stop()





