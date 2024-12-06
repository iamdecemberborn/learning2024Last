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





