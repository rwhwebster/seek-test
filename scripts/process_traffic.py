import logging
import os
import sys
import tempfile

from pyspark.sql import SparkSession

from seek_traffic import ingestion, traffic_transformer

LOG_FILENAME = "project.log"
APP_NAME = "Traffic Counting"


if __name__ == "__main__":
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    logging.info(sys.argv)

    if len(sys.argv) is not 3:
        logging.warning("Input source and output path are required")
        sys.exit(1)

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
    app_name = sc.appName
    logging.info("Application Initialized: " + app_name)
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    transform_path = os.path.join(tempfile.mkdtemp(), "transform")

    ingestion.run(spark, input_path, transform_path)

    traffic_transformer.run(spark, transform_path, output_path)

    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
