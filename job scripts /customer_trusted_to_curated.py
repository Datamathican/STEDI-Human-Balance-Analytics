import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_landing
accelerometer_landing_node1766766425068 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-ps-123/accelerometer_landing/"]}, transformation_ctx="accelerometer_landing_node1766766425068")

# Script generated for node customer_trusted
customer_trusted_node1766766422934 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-ps-123/customer_trusted/"]}, transformation_ctx="customer_trusted_node1766766422934")

# Script generated for node Join
Join_node1766766609154 = Join.apply(frame1=accelerometer_landing_node1766766425068, frame2=customer_trusted_node1766766422934, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1766766609154")

# Script generated for node Drop Duplicates
DropDuplicates_node1766766754010 =  DynamicFrame.fromDF(Join_node1766766609154.toDF().dropDuplicates(["email"]), glueContext, "DropDuplicates_node1766766754010")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1766766754010, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766765029894", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766766865781 = glueContext.getSink(path="s3://stedi-lakehouse-ps-123/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766766865781")
AmazonS3_node1766766865781.setCatalogInfo(catalogDatabase="default",catalogTableName="customer_curated")
AmazonS3_node1766766865781.setFormat("json")
AmazonS3_node1766766865781.writeFrame(DropDuplicates_node1766766754010)
job.commit()
