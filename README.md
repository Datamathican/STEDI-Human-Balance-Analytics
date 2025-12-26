STEDI Human Balance Analytics: AWS Data Lakehouse Project

📌 Project Overview
The STEDI team is developing a hardware-enabled mobile app called "STEDI Step Trainer" that helps users train their balance. This project involves building a multi-zone Data Lakehouse using AWS Glue, S3, and Athena to process data from three sources:

Customer Data: Profile information and research consent.

Accelerometer Data: Real-time movement captured via mobile sensors.

Step Trainer Data: IoT sensor readings from the hardware device.

The final goal is to produce a "Curated" dataset for Machine Learning models to accurately predict user balance.

🏗️ Data Architecture
This project follows the Medallion Architecture to ensure data quality and lineage:
Opens in a new window
dev.to

Visual ETL Pipeline in AWS Glue Studio

Landing Zone: Raw JSON data stored in S3. No filtering or cleaning.

Trusted Zone: Filtered data. Only includes customers who agreed to share data for research.

Curated Zone: Business-level tables. Specifically handles the "Serial Number Bug" and joins sensor data for ML training.

🛠️ Tech Stack
Storage: Amazon S3

Compute: AWS Glue (PySpark ETL Jobs)

Query Engine: Amazon Athena

Cataloging: AWS Glue Data Catalog & Crawlers

🔍 Key Challenges & Solutions
1. The "13-Million-Row Explosion"

Observation: During the creation of the step_trainer_trusted table, the row count unexpectedly skyrocketed to over 13 million records. Cause: The join was matching against a customer_curated table that contained duplicate serial numbers. Because many accelerometer readings existed for each customer, a many-to-many relationship was accidentally created. Resolution: I implemented a Drop Duplicates transform in the prior job, ensuring each customer/serial number was unique. This successfully brought the final count down to the target 14,460 rows.

2. Resolving Ambiguous Schema Joins

Observation: Standard "Join" nodes in Glue Studio often threw errors regarding "overlapping field names" because serialNumber existed in both source tables. Resolution: I pivoted to using SQL Query Transform Nodes. This allowed for precise aliasing (e.g., s.serialNumber vs c.serialNumber) and cleaner column selection, which proved more stable for large-scale joins.
Opens in a new window
docs.aws.amazon.com

Configuration of the SQL Query Transform node

📊 Final Data Audit
After resolving the data quality issues, the final row counts confirmed a successful pipeline:
Opens in a new window


Final Athena verification query results

Table	Zone	Final Row Count	Purpose
customer_trusted	Trusted	482	Filtered by research consent
accelerometer_trusted	Trusted	40,981	Filtered by user consent
customer_curated	Curated	482	Unique users with validated serial numbers
step_trainer_trusted	Trusted	14,460	Validated device data (482 users × 30 readings)
machine_learning_curated	Curated	43,681	Final ML Feature Set (Joined sensor data)

📁 Repository Contents
/scripts: Python/PySpark scripts for all 5 AWS Glue Jobs.

/sql: DDL scripts for table creation and the final Audit Query.

/screenshots: Visual proof of successful job runs and table counts.

🚀 How to Run
Upload raw data to your Landing S3 folders.

Run the Glue Crawlers to populate the initial Data Catalog.

Execute the jobs in order: customer_landing_to_trusted ➔ accelerometer_landing_to_trusted ➔ customer_trusted_to_curated ➔ step_trainer_landing_to_trusted ➔ machine_learning_curated.

Verify results using the provided SQL Audit script in Amazon Athena.

Created as part of the Udacity Data Engineering NanoDegr
