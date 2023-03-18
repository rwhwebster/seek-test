import csv
import os
import tempfile
from datetime import datetime, timedelta
from random import randint
from typing import List, Tuple

from pyspark.sql.types import IntegerType, StructField, StructType, TimestampType

from seek_traffic import ingestion
from tests.integration import SPARK


def test_data_ingestion_parsing():
    """
    The data ingestion section of the program should read the space-separated
    data and parse data to output data with the correct schema and types
    """

    ingest_folder, transform_folder = __create_ingest_and_transform_folders()
    input_csv_path = ingest_folder + "input.csv"

    end_time = datetime.now().replace(microsecond=0, second=0, minute=0)
    csv_content = [
        [end_time - timedelta(minutes=30 * i), randint(0, 30)] for i in range(20)
    ]

    __write_csv_file(input_csv_path, csv_content)

    ingestion.run(SPARK, input_csv_path, transform_folder)

    actual = SPARK.read.parquet(transform_folder)

    expected_schema = StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("count", IntegerType(), True),
        ]
    )

    expected = SPARK.createDataFrame(csv_content, schema=expected_schema)

    assert expected.collect() == actual.collect()


def __create_ingest_and_transform_folders() -> Tuple[str, str]:
    base_path = tempfile.mkdtemp()
    ingest_folder = "%s%s" % (base_path, os.path.sep)
    transform_folder = "%s%stransform" % (base_path, os.path.sep)
    return ingest_folder, transform_folder


def __write_csv_file(file_path: str, content: List[List[str]]) -> None:
    with open(file_path, "w") as csv_file:
        input_csv_writer = csv.writer(csv_file, delimiter=" ")
        input_csv_writer.writerows(content)
        csv_file.close()
