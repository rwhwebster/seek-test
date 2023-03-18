#!/bin/bash

set -e

poetry build

INPUT_FILE_PATH="./data/cars.csv"
OUTPUT_PATH="./output"

rm -rf $OUTPUT_PATH

poetry run spark-submit \
    --master local \
    --py-files dist/seek-test-*.whl \
    scripts/process_traffic.py \
    $INPUT_FILE_PATH \
    $OUTPUT_PATH
