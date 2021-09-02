from prefect import Flow
from prefect import Parameter
from prefect import task

from caching_flow_runner.test_utils.memory_result import MemoryResult


@task(result=MemoryResult(), checkpoint=True, target="{task_name}.pkl")
def get(a):
    return a


@task(result=MemoryResult(), checkpoint=True, target="{task_name}.pkl")
def inc(b):
    return b + 1


@task()
def multiply(c):
    return c * 2


with Flow("test") as flow:
    p = Parameter("p")
    g = get(p)
    i = inc(g)
    m = multiply(i)
