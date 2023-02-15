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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1676474164611 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ali-bucket-udacity-stedi/project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1676474164611",
)

# Script generated for node Renamed keys for Customer Privacy Join
RenamedkeysforCustomerPrivacyJoin_node1676484270771 = ApplyMapping.apply(
    frame=StepTrainerLanding_node1676474164611,
    mappings=[
        ("sensorReadingTime", "long", "stepTrainerSensorReadingTime", "long"),
        ("serialNumber", "string", "stepTrainerSerialNumber", "string"),
        ("distanceFromObject", "int", "stepTrainerDdistanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforCustomerPrivacyJoin_node1676484270771",
)

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=RenamedkeysforCustomerPrivacyJoin_node1676484270771,
    keys1=["serialNumber"],
    keys2=["stepTrainerSerialNumber"],
    transformation_ctx="CustomerPrivacyJoin_node2",
)

# Script generated for node Drop Customer Fields
DropCustomerFields_node3 = DropFields.apply(
    frame=CustomerPrivacyJoin_node2,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropCustomerFields_node3",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1676474731830 = glueContext.write_dynamic_frame.from_options(
    frame=DropCustomerFields_node3,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://ali-bucket-udacity-stedi/project/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1676474731830",
)

job.commit()
