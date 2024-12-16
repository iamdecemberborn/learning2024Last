'''This AWS Glue script performs an ETL (Extract, Transform, Load) operation where it reads data from the AWS
Glue Data Catalog, applies a schema transformation, and writes the transformed data to Amazon S3 in Parquet
format with compression.'''
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





'''This AWS Glue script performs an ETL (Extract, Transform, Load) operation where it reads data from the AWS
Glue Data Catalog, applies a schema transformation, and writes the transformed data to Amazon S3 in Parquet
format with compression.'''
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



'''This Python script converts all Parquet files in a specified folder ("parquet") into CSV files, saving them in another folder ("outputcsv"). It uses 
the pandas library to handle file conversions and the os module for file and folder operations. The script defines a convert_parquet_to_csv function to 
ead a Parquet file into a Pandas DataFrame, save it as a CSV file, and print a confirmation message. The main() function ensures the output folder exists, 
iterates through all .parquet files in the input folder, and converts each to CSV by calling the conversion function. When executed as the main program, 
the script automates the entire process, printing success messages upon completion.'''
# import pandas as pd
# import os
# # Paths to input and output folders
# input_folder = "parquet"
# output_folder = "outputcsv"
# def convert_parquet_to_csv(parquet_file_path, csv_file_path):
#     # Read Parquet file
#     df = pd.read_parquet(parquet_file_path)
#     # Write DataFrame to CSV
#     df.to_csv(csv_file_path, index=False)
#     print(f"Converted {parquet_file_path} to {csv_file_path}")
# def main():
#     print("Starting the Parquet to CSV conversion script")
#     # Ensure the output folder exists
#     if not os.path.exists(output_folder):
#         os.makedirs(output_folder)
#     # Process each Parquet file in the input folder
#     for file_name in os.listdir(input_folder):
#         if file_name.endswith(".parquet"):
#             parquet_file_path = os.path.join(input_folder, file_name)
#             csv_file_path = os.path.join(output_folder, file_name.replace(".parquet", ".csv"))
#             convert_parquet_to_csv(parquet_file_path, csv_file_path)
#     print("Successfully converted all Parquet files to CSV format")
# if __name__ == '__main__':
#     main()







# import pandas as pd
# import os
# # Paths to input and output folders
# input_folder = "parquet"
# output_folder = "outputcsv"
# def convert_parquet_to_csv(parquet_file_path, csv_file_path):
#     # Read Parquet file
#     df = pd.read_parquet(parquet_file_path)
#     # Write DataFrame to CSV
#     df.to_csv(csv_file_path, index=False)
#     print(f"Converted {parquet_file_path} to {csv_file_path}")
# def main():
#     print("Starting the Parquet to CSV conversion script")
#     # Ensure the output folder exists
#     if not os.path.exists(output_folder):
#         os.makedirs(output_folder)
#     # Process each Parquet file in the input folder
#     for file_name in os.listdir(input_folder):
#         if file_name.endswith(".parquet"):
#             parquet_file_path = os.path.join(input_folder, file_name)
#             csv_file_path = os.path.join(output_folder, file_name.replace(".parquet", ".csv"))
#             convert_parquet_to_csv(parquet_file_path, csv_file_path)
#     print("Successfully converted all Parquet files to CSV format")
# if __name__ == '__main__':
#     main()




'''
This AWS Glue ETL job reads data from an S3 bucket using a Glue DynamicFrame and processes it step-by-step for transformations, 
filtering, and aggregations before saving the output back to S3 in Parquet format. The script starts by initializing the 
Glue job and setting up a logger for tracking the workflow. It then reads data from the Glue Data Catalog table 
(gluelearningdec2024product) as a DynamicFrame and logs its schema and row count. Using the ApplyMapping transform, 
the script renames and maps columns to a new schema. The ResolveChoice transform casts the new_seller_id column from a 
string to a long, replacing invalid values with nulls. The DynamicFrame is converted into a Spark DataFrame, and rows with 
null values in new_seller_id are filtered out. A new column, new_status, is added with a static value "Active". The filtered 
DataFrame is registered as a temporary SQL view (product_view) to run a SQL query that groups the data by new_year, calculating 
the count of customers and the sum of quantities. The aggregated results are converted back into a DynamicFrame and written to an S3 
bucket in Parquet format at s3://gluelearningdec2024/output/. Finally, the job logs the success of the ETL process and commits the job. 
This workflow performs schema mapping, data validation, filtering, column addition, SQL-based aggregation, and storage of processed 
data efficiently.'''
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
#                                                       transformation_ctx="ResolveChoice_node"
#                                                       )
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




'''the output will be have some logs looks like below and the above code is commented which ir running in aws cosole '''
# My log message
# print schema of S3bucket_node1
# root
# |-- marketplace: string
# |-- customer_id: long
# |-- product_id: string
# |-- seller_id: string
# |-- sell_date: string
# |-- quantity: long
# |-- year: string
#
# Number of rows in S3bucket_node1 dynamic frame:  11
# count for frame is 11
# print schema of ResolveChoice_node
# root
# |-- new_marketplace: string
# |-- new_customer_id: long
# |-- new_product_id: string
# |-- new_seller_id: long
# |-- new_sell_date: string
# |-- new_quantity: long
# |-- new_year: string
#
# convert dynamic dataframe ResolveChoice_node into spark dataframe
# /opt/amazon/spark/python/lib/pyspark.zip/pyspark/sql/dataframe.py:127: UserWarning: DataFrame constructor is internal. Do not directly use it.
# filter rows with  where new_seller_id is not null
# create new column status with Active value
# +---------------+---------------+--------------+-------------+-------------+------------+--------+----------+
# |new_marketplace|new_customer_id|new_product_id|new_seller_id|new_sell_date|new_quantity|new_year|new_status|
# +---------------+---------------+--------------+-------------+-------------+------------+--------+----------+
# |             US|       49033728|   A6302503213|         1111|   31-08-2021|          10|    2021|    Active|
# |             US|       17857748|   B000059PET1|         2222|   20-09-2021|          20|    2021|    Active|
# |             US|       25551507|   S7888128071|         3333|   31-08-2021|          10|    2021|    Active|
# |             US|       21025041|    W630250993|         4444|   20-09-2021|          20|    2021|    Active|
# |             US|       40943563|    B00JENS2BI|         5555|   31-08-2021|          10|    2021|    Active|
# |             US|       17013969|   J6305761302|         6666|   05-09-2021|          30|    2021|    Active|
# |             US|       47611685|
# K6300157555|         7777|   06-09-2021|          30|    2021|    Active|
# |             US|        7777728|   F6A02503213|         8888|   31-08-2022|          10|    2022|    Active|
# |             US|        8888848|   HK90059PET1|         9999|   20-09-2022|          20|    2022|    Active|
# +---------------+---------------+--------------+-------------+-------------+------------+--------+----------+
#
# convert spark dataframe into table view product_view. so that we can run sql
# create dataframe by spark sql
# display records after aggregate result
# +--------+---+---+
# |new_year|cnt|qty|
# +--------+---+---+
# |    2022|  2| 30|
# |    2021|  7|130|
# +--------+---+---+
#
# convert spark dataframe to dynamic frame
# dynamic frame uploaded in bucket myglue-etl-project/output/newproduct/ in parquet format
# etl job processed successfully

