# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job

# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

# # Script generated for node AWS Glue Data Catalog
# AWSGlueDataCatalog_node1732982343949 = glueContext.create_dynamic_frame.from_catalog(database="gluelearningdec2024", table_name="gluelearningdec2024product", transformation_ctx="AWSGlueDataCatalog_node1732982343949")

# # Script generated for node Change Schema
# ChangeSchema_node1732982347635 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1732982343949, mappings=[("marketplace", "string", "marketplace", "string"), ("customer_id", "long", "customer_id", "long"), ("product_id", "string", "product_id", "string"), ("seller_id", "string", "seller_id", "string"), ("sell_date", "string", "sell_date", "string"), ("quantity", "long", "quantity", "long"), ("year", "string", "year", "string")], transformation_ctx="ChangeSchema_node1732982347635")

# # Script generated for node Amazon S3
# AmazonS3_node1732982351479 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1732982347635, connection_type="s3", format="glueparquet", connection_options={"path": "s3://gluelearningdec2024/output/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1732982351479")

# job.commit()




#############################################################################
#############################################################################
# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# # Parse job parameters
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# # Initialize Spark and Glue context
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# # Initialize Glue job
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
# # Load data from the AWS Glue Data Catalog
# data_catalog_frame = glueContext.create_dynamic_frame.from_catalog(
#     database="gluelearningdec2024",
#     table_name="gluelearningdec2024product",
#     transformation_ctx="data_catalog_frame"
# )
# # Apply schema mapping to transform the data
# mapped_frame = ApplyMapping.apply(
#     frame=data_catalog_frame,
#     mappings=[
#         ("marketplace", "string", "marketplace", "string"),
#         ("customer_id", "long", "customer_id", "long"),
#         ("product_id", "string", "product_id", "string"),
#         ("seller_id", "string", "seller_id", "string"),
#         ("sell_date", "string", "sell_date", "string"),
#         ("quantity", "long", "quantity", "long"),
#         ("year", "string", "year", "string")
#     ],
#     transformation_ctx="mapped_frame"
# )
# # Write the transformed data to Amazon S3 in Parquet format
# glueContext.write_dynamic_frame.from_options(
#     frame=mapped_frame,
#     connection_type="s3",
#     format="glueparquet",
#     connection_options={
#         "path": "s3://gluelearningdec2024/output/",
#         "partitionKeys": []
#     },
#     format_options={"compression": "snappy"},
#     transformation_ctx="s3_write"
# )
# # Commit the Glue job
# job.commit()



#####################################################################
#####################################################################


# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import lit
# from awsglue.dynamicframe import DynamicFrame
# import logging

# logger = logging.getLogger('my_logger')
# logger.setLevel(logging.INFO)

# # Create a handler for CloudWatch
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('My log message')

# args = getResolvedOptions(sys.argv, ["JOB_NAME"])
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args["JOB_NAME"], args)

# # Script generated for node S3 bucket
# S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
#     database="gluelearningdec2024", table_name="gluelearningdec2024product", transformation_ctx="S3bucket_node1"
# )
# logger.info('print schema of S3bucket_node1')
# S3bucket_node1.printSchema()
# count = S3bucket_node1.count()
# print("Number of rows in S3bucket_node1 dynamic frame: ", count)
# logger.info('count for frame is {}'.format(count))

# # Script generated for node ApplyMapping
# ApplyMapping_node2 = ApplyMapping.apply(
#     frame=S3bucket_node1,
#     mappings=[
#         ("marketplace", "string", "new_marketplace", "string"),
#         ("customer_id", "long", "new_customer_id", "long"),
#         ("product_id", "string", "new_product_id", "string"),
#         ("seller_id", "string", "new_seller_id", "string"),
#         ("sell_date", "string", "new_sell_date", "string"),
#         ("quantity", "long", "new_quantity", "long"),
#         ("year", "string", "new_year", "string"),
#     ],
#     transformation_ctx="ApplyMapping_node2",
# )
# #convert those string values to long values using the resolveChoice transform method with a cast:long option:

# #This replaces the string values with null values
# ResolveChoice_node = ApplyMapping_node2.resolveChoice(specs = [('new_seller_id','cast:long')],
# transformation_ctx="ResolveChoice_node"
# )
# logger.info('print schema of ResolveChoice_node')
# ResolveChoice_node.printSchema()

# #convert dynamic dataframe into spark dataframe
# logger.info('convert dynamic dataframe ResolveChoice_node into spark dataframe')
# spark_data_frame=ResolveChoice_node.toDF()

# #apply spark where clause
# logger.info('filter rows with  where new_seller_id is not null')
# spark_data_frame_filter = spark_data_frame.where("new_seller_id is NOT NULL")

# # Add the new column to the data frame
# logger.info('create new column status with Active value')
# spark_data_frame_filter = spark_data_frame_filter.withColumn("new_status", lit("Active"))
# spark_data_frame_filter.show()
# logger.info('convert spark dataframe into table view product_view. so that we can run sql ')
# spark_data_frame_filter.createOrReplaceTempView("product_view")
# logger.info('create dataframe by spark sql ')
# product_sql_df = spark.sql("SELECT new_year,count(new_customer_id) as cnt,sum(new_quantity) as qty FROM product_view group by new_year ")
# logger.info('display records after aggregate result')
# product_sql_df.show()

# # Convert the data frame back to a dynamic frame
# logger.info('convert spark dataframe to dynamic frame ')
# dynamic_frame = DynamicFrame.fromDF(product_sql_df, glueContext, "dynamic_frame")
# logger.info('dynamic frame uploaded in bucket myglue-etl-project/output/newproduct/ in parquet format ')

# # Script generated for node S3 bucket
# S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
#     frame=dynamic_frame,
#     connection_type="s3",
#     format="glueparquet",
#     connection_options={
#         "path": "s3://gluelearningdec2024/output/",
#         "partitionKeys": [],
#     },
#     transformation_ctx="S3bucket_node3",
# )
# logger.info('etl job processed successfully')
# job.commit()




#####################################################################
#####################################################################
#
# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import lit
# from awsglue.dynamicframe import DynamicFrame
# import logging
#
# # Setting up logging to monitor and debug the ETL process
# logger = logging.getLogger('my_logger')
# logger.setLevel(logging.INFO)
#
# # Create a logging handler for output (e.g., CloudWatch or standard console)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
#
# # Log a basic message to verify logging is working
# logger.info('My log message')
#
# # Get the Glue job name from the command-line arguments
# args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#
# # Initialize the Spark and Glue contexts to interact with Spark and Glue
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
#
# # Initialize the Glue job for tracking execution status
# job = Job(glueContext)
# job.init(args["JOB_NAME"], args)
#
# # Read data from an S3 bucket registered in the Glue Data Catalog
# S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
#     database="gluelearningdec2024",  # The Glue database name
#     table_name="gluelearningdec2024product",  # The Glue table name
#     transformation_ctx="S3bucket_node1"  # Context name for tracking
# )
#
# # Log the schema of the data to understand its structure
# logger.info('print schema of S3bucket_node1')
# S3bucket_node1.printSchema()  # Prints the schema of the source data
#
# # Count the number of rows in the DynamicFrame for validation purposes
# count = S3bucket_node1.count()
# print("Number of rows in S3bucket_node1 dynamic frame: ", count)
# logger.info('count for frame is {}'.format(count))
#
# # Map the columns of the source data to new names and formats using ApplyMapping
# ApplyMapping_node2 = ApplyMapping.apply(
#     frame=S3bucket_node1,
#     mappings=[
#         ("marketplace", "string", "new_marketplace", "string"),
#         ("customer_id", "long", "new_customer_id", "long"),
#         ("product_id", "string", "new_product_id", "string"),
#         ("seller_id", "string", "new_seller_id", "string"),
#         ("sell_date", "string", "new_sell_date", "string"),
#         ("quantity", "long", "new_quantity", "long"),
#         ("year", "string", "new_year", "string"),
#     ],
#     transformation_ctx="ApplyMapping_node2"  # Context name for tracking
# )
#
# # Cast the 'new_seller_id' column from string to long, replacing invalid values with null
# ResolveChoice_node = ApplyMapping_node2.resolveChoice(
#     specs=[('new_seller_id', 'cast:long')],  # Specify casting rule for column
#     transformation_ctx="ResolveChoice_node"
# )
#
# # Log the schema after applying the casting to verify the changes
# logger.info('print schema of ResolveChoice_node')
# ResolveChoice_node.printSchema()
#
# # Convert the DynamicFrame to a Spark DataFrame for more advanced operations
# logger.info('convert dynamic dataframe ResolveChoice_node into spark dataframe')
# spark_data_frame = ResolveChoice_node.toDF()
#
# # Filter out rows where 'new_seller_id' is null for data quality purposes
# logger.info('filter rows with where new_seller_id is not null')
# spark_data_frame_filter = spark_data_frame.where("new_seller_id is NOT NULL")
#
# # Add a new column 'new_status' with a constant value "Active"
# logger.info('create new column status with Active value')
# spark_data_frame_filter = spark_data_frame_filter.withColumn("new_status", lit("Active"))
#
# # Display a sample of the filtered and updated DataFrame for validation
# spark_data_frame_filter.show()
#
# # Register the DataFrame as a temporary view to run SQL queries on it
# logger.info('convert spark dataframe into table view product_view. so that we can run sql ')
# spark_data_frame_filter.createOrReplaceTempView("product_view")
#
# # Run a Spark SQL query to group the data by 'new_year', count customers, and sum quantities
# logger.info('create dataframe by spark sql ')
# product_sql_df = spark.sql("SELECT new_year, count(new_customer_id) as cnt, sum(new_quantity) as qty FROM product_view GROUP BY new_year")
#
# # Display the aggregated results to verify correctness
# logger.info('display records after aggregate result')
# product_sql_df.show()
#
# # Convert the resulting Spark DataFrame back into a Glue DynamicFrame
# logger.info('convert spark dataframe to dynamic frame ')
# dynamic_frame = DynamicFrame.fromDF(product_sql_df, glueContext, "dynamic_frame")
#
# # Write the processed DynamicFrame to S3 in Parquet format
# logger.info('dynamic frame uploaded in bucket myglue-etl-project/output/newproduct/ in parquet format ')
# S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
#     frame=dynamic_frame,
#     connection_type="s3",  # Specify S3 as the target storage
#     format="glueparquet",  # Use Parquet format for efficient storage
#     connection_options={
#         "path": "s3://gluelearningdec2024/output/",  # S3 target path
#         "partitionKeys": []  # No partitioning is applied
#     },
#     transformation_ctx="S3bucket_node3"
# )
#
# # Log a success message indicating the ETL job completed successfully
# logger.info('etl job processed successfully')
#
# # Commit the Glue job to signal completion
# job.commit()


##################################################################################################


# # Import necessary libraries for Glue, Spark, and logging
# import sys
# from awsglue.transforms import *  # Provides Glue transformations like ApplyMapping
# from awsglue.utils import getResolvedOptions  # Utility to fetch job arguments
# from pyspark.context import SparkContext  # Spark entry point
# from awsglue.context import GlueContext  # Glue-specific context for handling Glue operations
# from awsglue.job import Job  # Manages the Glue job lifecycle
# from pyspark.sql.functions import lit  # Used to create new columns with constant values
# from awsglue.dynamicframe import DynamicFrame  # Glue abstraction for semi-structured data
# import logging  # Enables logging for tracking and debugging
#
# # Set up logging for monitoring and debugging
# logger = logging.getLogger('my_logger')  # Create a logger instance
# logger.setLevel(logging.INFO)  # Set log level to INFO
# handler = logging.StreamHandler()  # Log output to the console/stream
# handler.setLevel(logging.INFO)  # Ensure handler captures INFO-level messages
# logger.addHandler(handler)  # Attach the handler to the logger
# logger.info('My log message')  # Log a test message
#
# # Get the Glue job name from command-line arguments
# args = getResolvedOptions(sys.argv, ["JOB_NAME"])  # Fetch the JOB_NAME argument
#
# # Initialize Spark and Glue contexts
# sc = SparkContext()  # Initialize Spark for distributed data processing
# glueContext = GlueContext(sc)  # Extend SparkContext with Glue-specific functionality
# spark = glueContext.spark_session  # Access Spark session for SQL and DataFrame operations
# job = Job(glueContext)  # Create a Glue job instance to track execution
# job.init(args["JOB_NAME"], args)  # Initialize the Glue job with the provided JOB_NAME
#
# # Read data from Glue Data Catalog (linked to an S3 bucket)
# S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
#     database="gluelearningdec2024",  # Glue database name
#     table_name="gluelearningdec2024product",  # Glue table name
#     transformation_ctx="S3bucket_node1"  # Context for tracking this transformation
# )
# logger.info('print schema of S3bucket_node1')  # Log schema display
# S3bucket_node1.printSchema()  # Print the schema of the input data to understand its structure
#
# # Count and log the number of rows in the input data
# count = S3bucket_node1.count()  # Get the number of rows
# print("Number of rows in S3bucket_node1 dynamic frame: ", count)  # Print the count
# logger.info('count for frame is {}'.format(count))  # Log the row count
#
# # Apply schema mapping to rename and map columns
# ApplyMapping_node2 = ApplyMapping.apply(
#     frame=S3bucket_node1,
#     mappings=[  # Map input columns to new names and types
#         ("marketplace", "string", "new_marketplace", "string"),
#         ("customer_id", "long", "new_customer_id", "long"),
#         ("product_id", "string", "new_product_id", "string"),
#         ("seller_id", "string", "new_seller_id", "string"),
#         ("sell_date", "string", "new_sell_date", "string"),
#         ("quantity", "long", "new_quantity", "long"),
#         ("year", "string", "new_year", "string"),
#     ],
#     transformation_ctx="ApplyMapping_node2",  # Context for tracking this transformation
# )
#
# # Resolve schema conflicts by casting new_seller_id to a long type
# ResolveChoice_node = ApplyMapping_node2.resolveChoice(
#     specs=[('new_seller_id', 'cast:long')],  # Define the column and desired cast type
#     transformation_ctx="ResolveChoice_node"  # Context for tracking this transformation
# )
# logger.info('print schema of ResolveChoice_node')  # Log schema after resolving choices
# ResolveChoice_node.printSchema()  # Print schema to verify changes
#
# # Convert the DynamicFrame to a Spark DataFrame for advanced transformations
# logger.info('convert dynamic dataframe ResolveChoice_node into spark dataframe')
# spark_data_frame = ResolveChoice_node.toDF()  # Convert to Spark DataFrame
#
# # Filter rows where new_seller_id is not null to ensure data quality
# logger.info('filter rows with where new_seller_id is not null')
# spark_data_frame_filter = spark_data_frame.where("new_seller_id IS NOT NULL")  # Apply filter
#
# # Add a new column "new_status" with a constant value "Active"
# logger.info('create new column status with Active value')
# spark_data_frame_filter = spark_data_frame_filter.withColumn("new_status", lit("Active"))  # Add column
#
# # Display the filtered and updated DataFrame for validation
# spark_data_frame_filter.show()  # Show a sample of the data
#
# # Register the DataFrame as a temporary SQL view to run SQL queries
# logger.info('convert spark dataframe into table view product_view. so that we can run sql ')
# spark_data_frame_filter.createOrReplaceTempView("product_view")  # Create SQL view
#
# # Perform an SQL aggregation query: group by new_year, count customers, sum quantities
# logger.info('create dataframe by spark sql ')
# product_sql_df = spark.sql(
#     "SELECT new_year, count(new_customer_id) as cnt, sum(new_quantity) as qty FROM product_view GROUP BY new_year"
# )
# logger.info('display records after aggregate result')  # Log after aggregation
# product_sql_df.show()  # Show the aggregated results
#
# # Convert the aggregated Spark DataFrame back to a Glue DynamicFrame
# logger.info('convert spark dataframe to dynamic frame ')
# dynamic_frame = DynamicFrame.fromDF(product_sql_df, glueContext, "dynamic_frame")  # Convert back
#
# # Write the processed DynamicFrame to S3 in Parquet format
# logger.info('dynamic frame uploaded in bucket myglue-etl-project/output/newproduct/ in parquet format ')
# S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
#     frame=dynamic_frame,  # Data to be written
#     connection_type="s3",  # Specify S3 as the output destination
#     format="glueparquet",  # Use Parquet format for storage efficiency
#     connection_options={
#         "path": "s3://gluelearningdec2024/output/",  # S3 path for saving the data
#         "partitionKeys": [],  # No partitioning applied
#     },
#     transformation_ctx="S3bucket_node3"  # Context for tracking this transformation
# )
#
# # Log a success message and commit the Glue job
# logger.info('etl job processed successfully')  # Log job completion
# job.commit()  # Mark the Glue job as completed
#
#
