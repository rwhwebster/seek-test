from datetime import datetime, timedelta

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StructField, StructType, TimestampType

from seek_traffic.traffic_transformer import (
    find_busiest_periods,
    find_quietest_90min_period,
    total_from_counts,
    totals_by_date,
)
from tests.unit import SPARK


@pytest.fixture
def test_data() -> DataFrame:
    """
    Creates a dataset for testing that contains data spanning 3 different days
    Counts are ascending throughout the dataset, so the quietest times are earliest
    and the busiest times latest.
    """
    schema = StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("count", IntegerType(), True),
        ]
    )

    start_time = datetime(year=2023, month=3, day=1, hour=12, minute=30)

    data = SPARK.createDataFrame(
        [[start_time + timedelta(minutes=30 * i), i] for i in range(100)], schema=schema
    )

    return data


def test_total_calculation(test_data):
    """
    Given a dataframe with the half-hourly timestamps and counts for each period, the
    program should be able to determine the total number of cars counted through the
    entire file.
    """
    result: int = total_from_counts(test_data).collect()[0][0]

    assert result == sum(range(100))


def test_sum_cars_by_date_calculation(test_data):
    """
    The program should be able to produce a dataframe that summarises the
    total number of cars counted on each day.
    """
    result: DataFrame = totals_by_date(test_data)
    assert len(result.collect()) == 3


def test_top_three_periods_calculation(test_data):
    """"""
    busy_times: DataFrame = find_busiest_periods(test_data)

    assert (
        busy_times.collect()
        == test_data.sort(f.col("timestamp").desc()).limit(3).collect()
    )


def test_quietest_90_mins_calculation(test_data):
    """
    Test calculation to find the quietest 90 minute period.
    This should be the first timestamp in the test dataset.
    """
    quiet_time: DataFrame = find_quietest_90min_period(test_data)

    assert quiet_time.collect()[0][0] == test_data.collect()[0][0]
