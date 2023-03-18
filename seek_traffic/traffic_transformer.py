import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql import window as w


def total_from_counts(data: DataFrame) -> DataFrame:
    """
    Return as a single integer the sum total of all counted cars in
    the dataset.
    """
    total = data.groupBy().sum()
    total.show()
    return total


def totals_by_date(data: DataFrame) -> DataFrame:
    """
    Returns a dataset where counts are grouped by date.
    """
    data_by_date: DataFrame = (
        data.withColumn("date", f.to_date(f.col("timestamp")))
        .groupBy(f.col("date"))
        .agg(f.sum("count").alias("total"))
    )
    data_by_date.show()
    data_by_date.printSchema()
    return data_by_date


def find_busiest_periods(data: DataFrame) -> DataFrame:
    """
    Sort each recorded period according to the number of cars
    counted from highest to lowest, returning the 3 busiest times
    """
    busy_times = data.sort(f.col("count").desc()).limit(3)
    busy_times.show()
    return busy_times


def find_quietest_90min_period(data: DataFrame) -> DataFrame:
    """
    Use an ordering window to get rolling totals for 90 minute time periods
    Sort the resultant list to get the timestamp with the lowest total
    """
    # Set up 90 minute window function on timestamps as seconds
    window = w.Window.orderBy(f.col("timestamp").cast("long")).rangeBetween(
        0, 90 * 60 - 1
    )

    # Perform a sum on count using the window
    windowed_data = data.withColumn("rolling_total", f.sum("count").over(window))

    windowed_data.show()

    return windowed_data.sort(f.col("rolling_total").asc()).limit(1).drop("count")


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    input_dataset = spark.read.parquet(input_path)
    input_dataset.show()

    total: DataFrame = total_from_counts(input_dataset)
    daily_totals: DataFrame = totals_by_date(input_dataset)
    busy_times: DataFrame = find_busiest_periods(input_dataset)
    quietest_time: DataFrame = find_quietest_90min_period(input_dataset)

    # Write outputs to files
    total.repartition(1).write.csv(
        os.path.join(output_path, "total.csv"), sep=" ", header=False
    )
    daily_totals.repartition(1).write.csv(
        os.path.join(output_path, "daily_totals.csv"), sep=" ", header=False
    )
    busy_times.repartition(1).write.csv(
        os.path.join(output_path, "busy_times.csv"), sep=" ", header=False
    )
    quietest_time.repartition(1).write.csv(
        os.path.join(output_path, "quietest_time.csv"), sep=" ", header=False
    )
