# STEDI Step Trainer Data Lakehouse to Train Machine Learning Models

## Project Overview

The STEDI Team has been hard at work developing a hardware **STEDI Step Trainer** that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.
  STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time.

## Project Summary

As a data engineer on the **STEDI Step Trainer** team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Project Data

STEDI has three JSON data sources to use from the Step Trainer. You can download the data from here or you can extract it from their respective public S3 bucket locations:

1. **Customer Records (from fulfillment and the STEDI website):**

**_AWS S3 Bucket URI - s3://cd0030bucket/customers/_**

contains the following fields:

- serialnumber
- sharewithpublicasofdate
- birthday
- registrationdate
- sharewithresearchasofdate
- customername
- email
- lastupdatedate
- phone
- sharewithfriendsasofdate

2. **Step Trainer Records (data from the motion sensor):**

**_AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/_**

contains the following fields:

- sensorReadingTime
- serialNumber
- distanceFromObject

3. **Accelerometer Records (from the mobile app):**

**_AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/_**

contains the following fields:

- timeStamp
- serialNumber
- x
- y
- z
