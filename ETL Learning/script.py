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
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# Parse job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Initialize Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Load data from the AWS Glue Data Catalog
data_catalog_frame = glueContext.create_dynamic_frame.from_catalog(
    database="gluelearningdec2024",
    table_name="gluelearningdec2024product",
    transformation_ctx="data_catalog_frame"
)
# Apply schema mapping to transform the data
mapped_frame = ApplyMapping.apply(
    frame=data_catalog_frame,
    mappings=[
        ("marketplace", "string", "marketplace", "string"),
        ("customer_id", "long", "customer_id", "long"),
        ("product_id", "string", "product_id", "string"),
        ("seller_id", "string", "seller_id", "string"),
        ("sell_date", "string", "sell_date", "string"),
        ("quantity", "long", "quantity", "long"),
        ("year", "string", "year", "string")
    ],
    transformation_ctx="mapped_frame"
)
# Write the transformed data to Amazon S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=mapped_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://gluelearningdec2024/output/",
        "partitionKeys": []
    },
    format_options={"compression": "snappy"},
    transformation_ctx="s3_write"
)
# Commit the Glue job
job.commit()


