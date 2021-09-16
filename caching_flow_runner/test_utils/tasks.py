import json

from prefect import Flow
from prefect import Parameter
from prefect import task

from caching_flow_runner.test_utils import RESOURCES
from caching_flow_runner.test_utils.memory_result import MemoryResult


memory_result = MemoryResult()


@task(result=memory_result, checkpoint=True)
def get(n):
    filename = f"{RESOURCES}/{n}.json"
    return json.loads(open(filename).read())


@task(result=memory_result, checkpoint=True)
def inc(b):
    return b + 1


@task()
def multiply(c):
    return c * 2


with Flow("test") as test_flow:
    p = Parameter("p")
    g = get(p)
    i = inc(g)
    m = multiply(i)

flow_lock = {}
