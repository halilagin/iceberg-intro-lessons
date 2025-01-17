#!/usr/bin/env python
# coding: utf-8

import yaml
import pyspark
from pyspark.sql import SparkSession
import boto3

# DEFINE SENSITIVE VARIABLES
NESSIE_URI = "http://nessie:19120/api/v1"
MINIO_ENDPOINT_URL = "http://minio:9000"
minio_keys_file = "../../secrets/minio.keys"


minio_conf = None
with open(minio_keys_file) as f:
    minio_conf = yaml.safe_load(f)

MINIO_ACCESS_KEY = minio_conf["access_key"]
MINIO_SECRET_KEY = minio_conf["secret_key"]


def check_minio_credentials(endpoint_url: str, access_key: str, secret_key: str) -> bool:
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",        # or any region name, required by some clients
    )

    try:
        response = s3_client.list_buckets()
        print("Buckets on MinIO/S3:")
        for bucket in response['Buckets']:
            print(f"    - {bucket['Name']}")
        return True
    except Exception as e:
        print("Error listing buckets with provided credentials:", e)
        return False


def run_spark():
    conf = (
        pyspark.SparkConf()
        .setAppName('app_name')
        .set(
                'spark.jars.packages',
                'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,'
                'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,'
                'software.amazon.awssdk:bundle:2.17.178,'
                'software.amazon.awssdk:url-connection-client:2.17.178'
        ).set(
                'spark.sql.extensions',
                'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'
                'org.projectnessie.spark.extensions.NessieSparkSessionExtensions'
        ).set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', NESSIE_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse', 's3a://warehouse')
        .set('spark.sql.catalog.nessie.s3.endpoint', MINIO_ENDPOINT_URL)
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY)
        .set('spark.sql.catalog.nessie.s3.access-key-id', MINIO_ACCESS_KEY)
        .set('spark.sql.catalog.nessie.s3.secret-access-key', MINIO_SECRET_KEY)
        .set('spark.sql.catalog.nessie.s3.path-style-access', 'true')
        )

    # Start Spark Session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("Spark Running")

    csv_df = spark.read.format("csv").option("header", "true").load("../../datasets/df_open_2023.csv")
    csv_df.createOrReplaceTempView("csv_open_2023")
    spark.sql("CREATE TABLE IF NOT EXISTS nessie.df_open_2023_lesson2 USING iceberg AS SELECT * FROM csv_open_2023").show()
    spark.sql("SELECT * FROM nessie.df_open_2023_lesson2 limit 10").show()
    spark.sql("SELECT Count(*) as Total FROM nessie.df_open_2023_lesson2").show()
    spark.sql("CREATE BRANCH IF NOT EXISTS lesson2 IN nessie")
    spark.sql("USE REFERENCE lesson2 IN nessie")
    spark.sql("DELETE FROM nessie.df_open_2023_lesson2 WHERE countryOfOriginCode = 'FR'")
    spark.sql("SELECT Count(*) as Total FROM nessie.df_open_2023_lesson2").show()
    spark.sql("USE REFERENCE main IN nessie")
    spark.sql("SELECT Count(*) as Total FROM nessie.df_open_2023_lesson2").show()
    spark.sql("MERGE BRANCH lesson2 INTO main IN nessie")
    spark.sql("SELECT Count(*) as Total FROM nessie.df_open_2023_lesson2").show()


if __name__ == "__main__":
    success = check_minio_credentials(MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    print("MinIO/S3 credentials are valid:", success)
    if success:
        run_spark()
