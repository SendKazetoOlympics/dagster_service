from dagster import job, op


@op
def return_five():
    return 5


@op
def add_one(arg):
    return arg + 1

