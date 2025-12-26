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

# Script generated for node customer_curated
customer_curated_node1766787482268 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="customer_curated", transformation_ctx="customer_curated_node1766787482268")

# Script generated for node step_trainer_landing
step_trainer_landing_node1766787451812 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1766787451812")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    s.sensorreadingtime, 
    s.serialnumber, 
    s.distancefromobject
FROM step_trainer_landing s
JOIN customer_curated c ON s.serialnumber = c.serialnumber;
'''
SQLQuery_node1766789199762 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"default_step_trainer_landing":step_trainer_landing_node1766787451812, "default_step_trainer_landing":customer_curated_node1766787482268}, transformation_ctx = "SQLQuery_node1766789199762")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1766789199762, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766783704042", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766787929934 = glueContext.getSink(path="s3://stedi-lakehouse-ps-123/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766787929934")
AmazonS3_node1766787929934.setCatalogInfo(catalogDatabase="default",catalogTableName="step_trainer_trusted")
AmazonS3_node1766787929934.setFormat("json")
AmazonS3_node1766787929934.writeFrame(SQLQuery_node1766789199762)
job.commit()
