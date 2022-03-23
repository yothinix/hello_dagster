import csv

import requests
from dagster import op, get_dagster_logger, job


@op
def hello_cereal():
    response = requests.get('https://docs.dagster.io/assets/cereal.csv')
    lines = response.text.split('\n')
    cereals = [row for row in csv.DictReader(lines)]
    get_dagster_logger().info(f'Found {len(cereals)} cereals')


@job
def hello_cereal_job():
    hello_cereal()
