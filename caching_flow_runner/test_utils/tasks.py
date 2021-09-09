import prefect
from prefect import Flow
from prefect import Parameter
from prefect import task
from prefect.engine.signals import LOOP

from caching_flow_runner.task_runner import task_hashed_filename
from caching_flow_runner.test_utils.memory_result import MemoryResult


memory_result = MemoryResult()


@task(result=memory_result, checkpoint=True, target=task_hashed_filename)
def get(a):
    return a


@task(result=memory_result, checkpoint=True, target=task_hashed_filename)
def inc(b):
    return b + 1


@task()
def multiply(c):
    return c * 2


@task(result=memory_result, checkpoint=True, target=task_hashed_filename)
def looping_task(n):
    logger = prefect.context["logger"]
    logger.info("\n")
    value = prefect.context.get("task_loop_result", None) or 0
    logger.info(f"Running looping_task with {n=} and {value=}")

    if "task_loop_result" not in prefect.context:
        logger.info("Raising initial LOOP signal")
        raise LOOP(message="Looper-1", result=None)

    new_value = inc.run(value)

    if new_value > n:
        return value

    logger.info(f"Raising LOOP signal {value=}, {new_value=}")
    raise LOOP(message=f"Looper-{new_value}", result=new_value)


with Flow("test") as test_flow:
    p = Parameter("p")
    g = get(p)
    i = inc(g)
    m = multiply(i)

flow_lock = {}
