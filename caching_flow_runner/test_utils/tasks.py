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
    logger = prefect.context["logger"]
    value = prefect.context.get("task_loop_result", 1)
    logger.info(f"Running looping_task with {n=} and {value=}")

    if "task_loop_result" not in prefect.context:
        logger.info("Raising initial LOOP signal")
        raise LOOP(message="Looper-0", result=0)

    new_value = inc.run(value)

    if new_value > n:
        return value

    logger.info(f"Raising LOOP signal {value=}, {new_value}")
    raise LOOP(message=f"Looper-{value}", result=new_value)


with Flow("test") as test_flow:
    p = Parameter("p")
    g = get(p)
    i = inc(g)
    m = multiply(i)

flow_lock = {}
