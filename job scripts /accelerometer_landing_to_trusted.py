import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node customer_trusted
customer_trusted_node1766765236003 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-ps-123/customer_trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1766765236003")

# Script generated for node accelerometer_landing
accelerometer_landing_node1766765041262 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-ps-123/accelerometer_landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1766765041262")

# Script generated for node Join
Join_node1766765343418 = Join.apply(frame1=customer_trusted_node1766765236003, frame2=accelerometer_landing_node1766765041262, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1766765343418")

# Script generated for node Drop Fields
DropFields_node1766765463470 = DropFields.apply(frame=Join_node1766765343418, paths=["email", "phone"], transformation_ctx="DropFields_node1766765463470")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1766765463470, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766765029894", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766765765311 = glueContext.getSink(path="s3://stedi-lakehouse-ps-123/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766765765311")
AmazonS3_node1766765765311.setCatalogInfo(catalogDatabase="default",catalogTableName="accelerometer_trusted")
AmazonS3_node1766765765311.setFormat("json")
AmazonS3_node1766765765311.writeFrame(DropFields_node1766765463470)
job.commit()
