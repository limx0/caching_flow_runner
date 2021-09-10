import inspect
from typing import Any, Dict, Union

import prefect
from dask.base import tokenize
from prefect import Parameter
from prefect import Task
from prefect.engine import TaskRunner
from prefect.engine.result import Result
from prefect.engine.serializers import Serializer
from prefect.engine.state import Cached
from prefect.engine.state import Looped
from prefect.engine.state import State
from prefect.engine.state import Success
from prefect.tasks.core.resource_manager import ResourceCleanupTask
from prefect.tasks.core.resource_manager import ResourceInitTask
from prefect.tasks.core.resource_manager import ResourceSetupTask
from prefect.utilities.executors import tail_recursive

from caching_flow_runner.lock_storage import get_lock
from caching_flow_runner.lock_storage import set_lock


def task_qualified_name(task: Task):
    return f"{task.__module__}.{task.name}"


def task_hashed_filename(**kwargs) -> str:
    # TODO Add a fs prefix - don't just use /
    task_name = kwargs["task_hash_name"]
    lock = get_lock(key=task_name)
    raw_inputs = lock["raw_inputs"]
    key = tokenize(**{k: v.value for k, v in raw_inputs.items()})
    folder = ""
    if kwargs.get("task_loop_state") is not None:
        folder = f"{kwargs['task_loop_state']}/"
    fn = f"{task_name}/{folder}{key}.pkl"
    return fn


def _clean_raw_inputs(task_name, raw_inputs):
    ignore_kwargs = (prefect.context.get("CachedTaskRunner_ignore_kwargs") or {}).get(
        task_name, tuple()
    )
    return {k: v for k, v in raw_inputs.items() if k not in ignore_kwargs}


def _hash_result(result: Any, serializer: Serializer) -> Dict:
    serialized = serializer.serialize(result)
    token = tokenize(result)
    return {"hash": token, "size": len(serialized)}


def _compare_input_hashes(inputs: Dict, lock: Dict):
    for key, result in inputs.items():
        if key not in lock:
            return False
        input_hash = _hash_result(result=result.value, serializer=result.serializer)
        lock_hash = lock[key]
        if input_hash != lock_hash:
            return False
    return True


def _hash_inputs(inputs: Dict[str, Union[Result, Parameter]]):
    return {
        key: _hash_result(value.value, serializer=value.serializer) for key, value in inputs.items()
    }


def _clean_source(source: str) -> str:
    """Remove any decorators from string of source code"""
    lines = source.split("\n")
    def_idx = next(idx for idx, line in enumerate(lines) if line.startswith("def "))
    return "\n".join(lines[def_idx:])


def _hash_source(func, name=None):
    """Hash source code for run function, stripping away @task decorator"""
    source = inspect.getsource(func)
    if source.startswith("@"):
        source = _clean_source(source)
    assert source.startswith(
        "def"
    ), f"Source for task {name} does not start with def, using decorator?"
    return _hash_result(result=source, serializer=SourceSerializer())


class CachedTaskRunner(TaskRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_full_name = task_qualified_name(task=self.task)
        self.state_handlers.append(self._on_state_change)
        self.last_state = None

    def _should_track(self):
        return not isinstance(
            self.task, (Parameter, ResourceInitTask, ResourceSetupTask, ResourceCleanupTask)
        )

    def _loop_task_name(self, message: str):
        return f"{self.task_full_name}-{message}"

    def _generate_task_lock(self, state: State):
        lock = get_lock(self.task_full_name)
        return {
            "inputs": _hash_inputs(inputs=lock["raw_inputs"]),
            "source": _hash_source(func=self.task.run, name=self.task.name),
            "result": _hash_result(result=state.result, serializer=self.result.serializer),
        }

    def _on_success(self, new_state):
        self.logger.info(f"Setting task lock for {self.task_full_name} based on {new_state}")
        task_lock = self._generate_task_lock(state=new_state)
        set_lock(self.task_full_name, task_lock)
        return new_state

    def _on_state_change(self, _, old_state: State, new_state: State) -> State:
        self.logger.info(f"on_state_change {old_state=} {new_state=}")
        prefect.context.update(last_state=new_state)
        if self._should_track():
            if isinstance(new_state, Success):
                return self._on_success(new_state=new_state)
            elif isinstance(new_state, Looped):
                self.logger.info("Looping task, setting task_hash_name to include loop message")
                prefect.context.update(task_hash_name=self._loop_task_name(new_state.message))
                self.logger.info(
                    f"{prefect.context.update(task_hash_name=self._loop_task_name(new_state.message))=}"
                )
        self.last_state = new_state
        return new_state

    def get_task_inputs(self, *args, **kwargs) -> Dict[str, Result]:
        task_inputs = super().get_task_inputs(*args, **kwargs)
        if self._should_track():
            lock = get_lock(self.task_full_name)
            lock["raw_inputs"] = _clean_raw_inputs(
                task_name=self.task_full_name, raw_inputs=task_inputs
            )
            # lock["loop_inputs"] = kwargs.get("state").result
            set_lock(self.task_full_name, lock)
        return task_inputs

    def check_task_is_cached(self, state: State, inputs: Dict[str, Result]) -> State:
        new_state = super().check_task_is_cached(state=state, inputs=inputs)

        # Additional check: result exists, need to check against lock
        if isinstance(state, Cached):
            lock = get_lock(self.task_full_name).get("inputs", {})
            if not _compare_input_hashes(inputs=inputs, lock=lock):
                return state

        return new_state

    def check_target(self, state: State, inputs: Dict[str, Result]) -> State:
        with prefect.context(last_state=self.last_state):
            return super().check_target(state=state, inputs=inputs)

    @tail_recursive
    def run(self, *args, **kwargs) -> State:
        with prefect.context(task_hash_name=self.task_full_name):
            return super().run(*args, **kwargs)

    def check_task_is_looping(self, *args, **kwargs) -> State:
        if isinstance(args[0], Looped):
            state: Looped = args[0]
            kwargs["context"].update(task_loop_state=state.result)
        new_state = super().check_task_is_looping(*args, **kwargs)
        return new_state


class SourceSerializer(Serializer):
    def serialize(self, value: Any) -> bytes:
        return value.encode()

    def deserialize(self, value: bytes) -> Any:
        return value.decode()
