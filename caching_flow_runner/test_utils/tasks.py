import prefect
from prefect import Flow
from prefect import Parameter
from prefect import task
from prefect.engine.signals import LOOP

from caching_flow_runner.test_utils.memory_result import MemoryResult


memory_result = MemoryResult()


@task(result=memory_result, checkpoint=True, target="{task_hash_name}.pkl")
def get(a):
    return a


@task(result=memory_result, checkpoint=True, target="{task_hash_name}.pkl")
def inc(b):
    return b + 1


@task()
def multiply(c):
    return c * 2


@task(result=memory_result, checkpoint=True, target="{task_hash_name}.pkl")
def looping_task(n):
    loop_payload = prefect.context.get("task_loop_result", {})
    value = loop_payload.get("value", 1)

    if "task_loop_result" not in prefect.context:
        raise LOOP(message="Looper-0", result=None)

    new_value = inc(value)

    if new_value > n:
        return value

    raise LOOP(message=f"Looper-{value}", result=dict(value=new_value))


with Flow("test") as test_flow:
    p = Parameter("p")
    g = get(p)
    i = inc(g)
    m = multiply(i)

flow_lock = {}
