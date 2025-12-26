import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1766789864654 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1766789864654")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1766789901968 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1766789901968")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.user,
    a.timestamp,
    a.x,
    a.y,
    a.z
FROM step_trainer_trusted s
JOIN accelerometer_trusted a ON s.sensorreadingtime = a.timestamp;
'''
SQLQuery_node1766789940018 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":step_trainer_trusted_node1766789864654, "accelerometer_trusted":accelerometer_trusted_node1766789901968}, transformation_ctx = "SQLQuery_node1766789940018")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1766789940018, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766783704042", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766790119762 = glueContext.getSink(path="s3://stedi-lakehouse-ps-123/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766790119762")
AmazonS3_node1766790119762.setCatalogInfo(catalogDatabase="default",catalogTableName="machine_learning_curated")
AmazonS3_node1766790119762.setFormat("json")
AmazonS3_node1766790119762.writeFrame(SQLQuery_node1766789940018)
job.commit()
