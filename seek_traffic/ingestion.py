import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StructField, StructType, TimestampType


def load_traffic_data_from_csv(spark: SparkSession, ingest_path: str) -> DataFrame:
    """ """
    logging.info("Reading data file from: %s", ingest_path)

    schema = StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("count", IntegerType(), True),
        ]
    )

    df = (
        spark.read.format("org.apache.spark.csv")
        .option("header", False)
        .option("delimiter", " ")
        .schema(schema)
        .csv(ingest_path)
    )
    return df


def run(spark: SparkSession, ingest_path: str, transformation_path: str) -> None:
    input_df = load_traffic_data_from_csv(spark, ingest_path)
    input_df.printSchema()
    input_df.show()

    input_df.write.parquet(transformation_path)
