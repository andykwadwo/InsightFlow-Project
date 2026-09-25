import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_flatten
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
AmazonS3_node1790084338337 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://calandly-market-bucket-oseikwadwo/calendly_spend_data_raw/"], "recurse": True}, transformation_ctx="AmazonS3_node1790084338337")

# Script generated for node Flatten
Flatten_node1790084633610 = AmazonS3_node1790084338337.gs_flatten(maxLevels=0)

# Script generated for node Change Schema
ChangeSchema_node1790084803054 = ApplyMapping.apply(frame=Flatten_node1790084633610, mappings=[("created_at", "string", "created_at", "date"), ("created_by", "string", "created_by", "date"), ("event", "string", "event", "string"), ("`payload.email`", "string", "payload_email", "string"), ("`payload.event`", "string", "payload_event", "string"), ("`payload.first_name`", "null", "first_name", "null"), ("`payload.invitee_scheduled_by`", "string", "invitee_scheduled_by", "string"), ("`payload.last_name`", "null", "last_name", "null"), ("`payload.name`", "string", "payload_name", "string"), ("`payload.reschedule_url`", "string", "payload_reschedule_url", "string"), ("`payload.rescheduled`", "boolean", "payload_rescheduled", "boolean"), ("`payload.scheduled_event.created_at`", "string", "`payload.scheduled_event.created_at`", "string"), ("`payload.scheduled_event.end_time`", "string", "`payload.scheduled_event.end_time`", "string"), ("`payload.scheduled_event.event_type`", "string", "`payload.scheduled_event.event_type`", "string"), ("`payload.scheduled_event.invitees_counter.total`", "int", "`payload.scheduled_event.invitees_counter.total`", "int"), ("`payload.scheduled_event.invitees_counter.active`", "int", "`payload.scheduled_event.invitees_counter.active`", "int"), ("`payload.scheduled_event.invitees_counter.limit`", "int", "`payload.scheduled_event.invitees_counter.limit`", "int"), ("`payload.scheduled_event.location.type`", "string", "`payload.scheduled_event.location.type`", "string"), ("`payload.scheduled_event.name`", "string", "`payload.scheduled_event.name`", "string"), ("`payload.scheduled_event.start_time`", "string", "`payload.scheduled_event.start_time`", "string"), ("`payload.scheduled_event.status`", "string", "`payload.scheduled_event.status`", "string"), ("`payload.scheduled_event.updated_at`", "string", "`payload.scheduled_event.updated_at`", "string"), ("`payload.scheduled_event.uri`", "string", "`payload.scheduled_event.uri`", "string"), ("`payload.status`", "string", "payload_status", "string"), ("`payload.timezone`", "string", "`payload.timezone`", "string")], transformation_ctx="ChangeSchema_node1790084803054")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1790084803054, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790084295768", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (ChangeSchema_node1790084803054.count() >= 1):
   ChangeSchema_node1790084803054 = ChangeSchema_node1790084803054.coalesce(1)
AmazonS3_node1790085439700 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1790084803054, connection_type="s3", format="csv", connection_options={"path": "s3://calandly-market-bucket-oseikwadwo/calendly_spend_data_silver/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1790085439700")

job.commit()