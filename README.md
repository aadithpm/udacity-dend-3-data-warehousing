# Data Warehouse for Sparkify

The goal of this project is to build a data warehouse for Sparkify:

* Data from the Million Songs dataset that consists of song and artist data
* Logs generated with this dataset and the event generator

The ETL is built with Python. The data is pulled from S3 buckets, transformed and loaded into a 4 node `dc2.large` Redshift cluster. An IAM user is defined for this whole process with permissions to read from S3 buckets and access Redshift clusters.

This documentation is split up into these following sections:

1. Table design and schemas
2. Data extraction and transformation
3. Explanation of files in the repository
4. Running the scripts

## 1. Table Design and Schemas

![ER Diagram](https://i.imgur.com/8zYZVo3.png)

## 2. Data extraction and transformation

For our PostgreSQL data modeling project, we processed each log and song data file separately, processing each row and loading it into the database. Here, we use COPY queries to access both log and song data stored in S3 buckets with the appropriate IAM role. The role in question should have permissions to read from S3 buckets. Post table creation, these COPY commands load data into our staging tables. 


After loading into our staging tables, we use standard INSERT queries to add data to our fact and dimension tables. 


## 3. Explanation of files in the repository

* `create_tables.py`: Script that drops and recreates tables. It does not define any queries of its own and imports queries from `sql_queries.py`

* `sql_queries.py`: List of queries for our project. This consists of `INSERT`, `COPY`, `DROP` and `CREATE` queries for all our staging, fact and dimension tables

* `dwh.ipynb`: Jupyter notebook that acts as a testing environment to check data integrity, AWS credentials, connection to our cluster and verify the correctness of our pipeline. Also contains analytics queries, which is the final goal of our pipeline

* `etl.py`: Run all `COPY` and `INSERT` queries defined in `sql_queries.py` to move data from S3 buckets -> staging tables -> fact & dimension tables

## 4. Running the scripts

To run the scripts:

**Inside a terminal:**

```
python create_tables.py
python etl.py
```

If the ETL process ran correctly, you should see as many `COPY` and `INSERT` queries as indicated at the start. The next step is to take a look at the simple analytics queries provided in `dwh.ipynb`. These queries try to gain some insight into how this data can help Sparkify as a product and learn some patterns about its userbase.

----
