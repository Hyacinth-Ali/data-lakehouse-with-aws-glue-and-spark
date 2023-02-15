import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="project_customer_landing",
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Customer Privace Filter
CustomerPrivaceFilter_node2 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="CustomerPrivaceFilter_node2",
)

# Script generated for node Customer Trusted
CustomerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=CustomerPrivaceFilter_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://ali-bucket-udacity-stedi/project/customers/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node3",
)

job.commit()
