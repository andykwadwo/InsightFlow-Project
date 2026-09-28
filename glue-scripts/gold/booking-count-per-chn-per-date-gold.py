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
AmazonS3_node1790551686804 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://calandly-market-bucket-oseikwadwo/calendly_spend_data_gold/"], "recurse": True}, transformation_ctx="AmazonS3_node1790551686804")

# Script generated for node SQL Query
SqlQuery2367 = '''
select
    channel,
    date,
    COUNT(*) AS booking_count_per_channel_per_date
from
    myDataSource
GROUP by
    channel,date


'''
SQLQuery_node1790551726410 = sparkSqlQuery(glueContext, query = SqlQuery2367, mapping = {"myDataSource":AmazonS3_node1790551686804}, transformation_ctx = "SQLQuery_node1790551726410")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1790551726410, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790550480232", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1790551726410.count() >= 1):
   SQLQuery_node1790551726410 = SQLQuery_node1790551726410.coalesce(1)
AmazonS3_node1790551965205 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1790551726410, connection_type="s3", format="csv", connection_options={"path": "s3://calandly-market-bucket-oseikwadwo/booking-trend-overtime-gold/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1790551965205")

job.commit()