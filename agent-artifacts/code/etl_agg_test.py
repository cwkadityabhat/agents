import sys
import boto3
import logging
from awsglue.utils import getResolvedOptions
from amorphicutils.pyspark.infra.gluespark import GlueSpark
from amorphicutils.pyspark import read
from amorphicutils.pyspark import write

# Set up logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# Initialize Glue context and Spark session
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
glue_spark = GlueSpark()
glue_context = glue_spark.get_glue_context()
spark = glue_spark.get_spark()

# Get S3 bucket names from Parameter Store
ssm_client = boto3.client("ssm")
lz_bucket_name = ssm_client.get_parameter(Name="SYSTEM.S3BUCKET.LZ", WithDecryption=False)["Parameter"]["Value"]
dlz_bucket_name = ssm_client.get_parameter(Name="SYSTEM.S3BUCKET.DLZ", WithDecryption=False)["Parameter"]["Value"]

# Define input and output parameters
USERID = "your_user_id"  # Replace with your Amorphic User ID
INPUT_DOMAIN = "rawtest"
INPUT_DATASET = "Immunization_Staged"
OUTPUT_DOMAIN = "output_domain"  # Replace with your desired output domain
OUTPUT_DATASET = "vaccine_manufacturer_count"  # Replace with your desired output dataset name

# Read input data
csv_reader = read.Read(dlz_bucket_name, spark=spark)
response = csv_reader.read_csv_data(
    domain_name=INPUT_DOMAIN,
    dataset_name=INPUT_DATASET,
    header=True
)

if response["exitcode"] != 0:
    LOGGER.error(f"Failed to read dataset: {response['message']}")
    raise Exception(response['message'])

df = response['data']

# Perform grouping and counting operation
result_df = df.groupBy("vaccine_manufacturer").count()

# Write results
csv_writer = write.Write(lz_bucket_name, glue_context)
write_response = csv_writer.write_csv_data(
    data=result_df,
    domain_name=OUTPUT_DOMAIN,
    dataset_name=OUTPUT_DATASET,
    user=USERID
)

if write_response["exitcode"] == 0:
    LOGGER.info(f"Successfully wrote results: {write_response['message']}")
else:
    LOGGER.error(f"Failed to write results: {write_response['message']}")
    raise Exception(write_response['message'])

LOGGER.info("Job completed successfully")