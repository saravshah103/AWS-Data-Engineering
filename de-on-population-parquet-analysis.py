import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1703064484266 = glueContext.create_dynamic_frame.from_catalog(
    database="de-population-cleaned-db",
    table_name="cleaned_population_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1703064484266",
)

# Script generated for node Amazon S3
AmazonS3_node1703064693396 = glueContext.getSink(
    path="s3://de-on-population-analytics-dev",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703064693396",
)
AmazonS3_node1703064693396.setCatalogInfo(
    catalogDatabase="db_population_analytics", catalogTableName="final_analytics"
)
AmazonS3_node1703064693396.setFormat("glueparquet")
AmazonS3_node1703064693396.writeFrame(AWSGlueDataCatalog_node1703064484266)
job.commit()
