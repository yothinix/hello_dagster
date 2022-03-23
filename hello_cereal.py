import csv

import requests
from dagster import op, get_dagster_logger, job


@op
def download_cereal():
    response = requests.get('https://docs.dagster.io/assets/cereal.csv')
    lines = response.text.split('\n')
    return [row for row in csv.DictReader(lines)]


@op
def find_sugariest(cereals):
    sorted_by_sugar = sorted(cereals, key=lambda cereal: cereal['sugars'])
    get_dagster_logger().info(f'{sorted_by_sugar[-1]["name"]} is the sugariest')


@job
def serial():
    find_sugariest(download_cereal())


if __name__ == '__main__':
    result = serial.execute_in_process()
