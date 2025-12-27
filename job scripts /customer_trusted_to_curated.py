import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


accelerometer_trusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"}, 
    connection_type="s3", 
    format="json", 
    connection_options={"paths": ["s3://stedi-lakehouse-ps-123/accelerometer_trusted/"]}, 
    transformation_ctx="accelerometer_trusted_node"
)


customer_trusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"}, 
    connection_type="s3", 
    format="json", 
    connection_options={"paths": ["s3://stedi-lakehouse-ps-123/customer_trusted/"]}, 
    transformation_ctx="customer_trusted_node"
)


Join_node = Join.apply(
    frame1=accelerometer_trusted_node, 
    frame2=customer_trusted_node, 
    keys1=["user"], 
    keys2=["email"], 
    transformation_ctx="Join_node"
)


customer_only_df = Join_node.toDF().drop("user", "timeStamp", "x", "y", "z").dropDuplicates(["email"])
DropDuplicates_node = DynamicFrame.fromDF(customer_only_df, glueContext, "DropDuplicates_node")


AmazonS3_node = glueContext.getSink(
    path="s3://stedi-lakehouse-ps-123/customer_curated/", 
    connection_type="s3", 
    updateBehavior="UPDATE_IN_DATABASE", 
    partitionKeys=[], 
    enableUpdateCatalog=True, 
    transformation_ctx="AmazonS3_node"
)
AmazonS3_node.setCatalogInfo(catalogDatabase="default", catalogTableName="customer_curated")
AmazonS3_node.setFormat("json")
AmazonS3_node.writeFrame(DropDuplicates_node)

job.commit()
