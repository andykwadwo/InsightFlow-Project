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

# Script generated for node Amazon S3
AmazonS3_node1790279600741 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://calandly-market-bucket-oseikwadwo/calendly_market_raw_data/"], "recurse": True}, transformation_ctx="AmazonS3_node1790279600741")

# Script generated for node Amazon S3
AmazonS3_node1790279553861 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://calandly-market-bucket-oseikwadwo/calendly_spend_data_silver/"], "recurse": True}, transformation_ctx="AmazonS3_node1790279553861")

# Script generated for node Join
Join_node1790279648853 = Join.apply(frame1=AmazonS3_node1790279600741, frame2=AmazonS3_node1790279553861, keys1=["date"], keys2=["created_at"], transformation_ctx="Join_node1790279648853")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1790279648853, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790279259094", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (Join_node1790279648853.count() >= 1):
   Join_node1790279648853 = Join_node1790279648853.coalesce(1)
AmazonS3_node1790280302088 = glueContext.write_dynamic_frame.from_options(frame=Join_node1790279648853, connection_type="s3", format="csv", connection_options={"path": "s3://calandly-market-bucket-oseikwadwo/calendly_spend_data_gold/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1790280302088")

job.commit()