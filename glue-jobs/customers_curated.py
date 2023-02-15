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

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ali-bucket-udacity-stedi/project/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1676474164611 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="project_accelerometer_landing2",
    transformation_ctx="AccelerometerLanding_node1676474164611",
)

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerLanding_node1676474164611,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacyJoin_node2",
)

# Script generated for node Drop Accelerometer Fields
DropAccelerometerFields_node3 = DropFields.apply(
    frame=CustomerPrivacyJoin_node2,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropAccelerometerFields_node3",
)

# Script generated for node Customer Curated
CustomerCurated_node1676474731830 = glueContext.write_dynamic_frame.from_options(
    frame=DropAccelerometerFields_node3,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://ali-bucket-udacity-stedi/project/customers/curated2/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1676474731830",
)

job.commit()
