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

# Script generated for node Amazon S3
AmazonS3_node1790550488298 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://calandly-market-bucket-oseikwadwo/calendly_spend_data_gold/"], "recurse": True}, transformation_ctx="AmazonS3_node1790550488298")

# Script generated for node SQL Query
SqlQuery2297 = '''
select
    channel,
    ROUND(SUM(spend),2) AS total_spent,
    COUNT(*) AS total_calls,
    ROUND((SUM(spend) / COUNT(*)),2) AS cost_per_booking 
from
    myDataSource
GROUP by
    channel


'''
SQLQuery_node1790550687913 = sparkSqlQuery(glueContext, query = SqlQuery2297, mapping = {"myDataSource":AmazonS3_node1790550488298}, transformation_ctx = "SQLQuery_node1790550687913")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1790550687913, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790550480232", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1790550687913.count() >= 1):
   SQLQuery_node1790550687913 = SQLQuery_node1790550687913.coalesce(1)
AmazonS3_node1790551307114 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1790550687913, connection_type="s3", format="csv", connection_options={"path": "s3://calandly-market-bucket-oseikwadwo/cpb-by-channel-gold/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1790551307114")

job.commit()