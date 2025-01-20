import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_path = "s3://laxmana/codepipeline/input/"

dyf_amazon_bestseller_phones = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options = {"paths": [source_path]},
    format_options = {"withHeader":True}
)

destination_path = "s3://laxmana/codepipeline/output/"

glueContext.write_dynamic_frame.from_options(
    frame = dyf_amazon_bestseller_phones,
    connection_type = "s3",
    connection_options = {"path": destination_path},
    format = "json"
)

job.commit()
