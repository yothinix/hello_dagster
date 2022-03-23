from dagster import job, op


@op
def get_name():
    return "dagster"


@op
def hello(name: str):
    print(f'Hello, {name}!')


@job
def hello_dagster():
    hello(get_name())


if __name__ == '__main__':
    result = hello_dagster.execute_in_process()
