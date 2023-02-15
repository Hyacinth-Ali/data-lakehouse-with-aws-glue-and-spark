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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="project_accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1676474164611 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ali-bucket-udacity-stedi/project/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1676474164611",
)

# Script generated for node Stet Trainer Accelerometer Join
StetTrainerAccelerometerJoin_node2 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainerTrusted_node1676474164611,
    keys1=["timestamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="StetTrainerAccelerometerJoin_node2",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1676474731830 = glueContext.write_dynamic_frame.from_options(
    frame=StetTrainerAccelerometerJoin_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://ali-bucket-udacity-stedi/project/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1676474731830",
)

job.commit()
