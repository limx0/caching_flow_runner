import hashlib
import inspect
from typing import Any, Dict

import prefect
from prefect import Parameter
from prefect import Task
from prefect.engine import TaskRunner
from prefect.engine.result import Result
from prefect.engine.serializers import Serializer
from prefect.engine.state import State
from prefect.engine.state import Success


LOCK = {}


def set_lock(key, value):
    global LOCK
    LOCK[key] = value


def get_lock(key=None):
    global LOCK
    if key is None:
        return LOCK.copy()
    return LOCK.get(key, {})


def clear_lock():
    global LOCK
    LOCK = {}


def task_qualified_name(task: Task):
    return f"{task.__module__}.{task.name}"


class CachedTaskRunner(TaskRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_full_name = task_qualified_name(task=self.task)

    def _should_track(self):
        return not isinstance(self.task, Parameter)

    def _hash_result(self, result: Any, serializer):
        serialized = serializer.serialize(result)
        hash = hashlib.md5(serialized).hexdigest()  # noqa: S303
        return {"md5": hash, "size": len(serialized)}

    def _check_input_hashes(self, inputs):
        lock = get_lock(self.task_full_name).get("inputs", {})
        for key, result in inputs.items():
            if key not in lock:
                return False
            input_hash = self._hash_result(result=result.value, serializer=result.serializer)
            lock_hash = lock[key]
            if input_hash != lock_hash:
                return False
        return True

    def check_target(self, state: State, inputs: Dict[str, Result]) -> State:
        if self._check_input_hashes(inputs=inputs):
            # Hashes match at this stage, can return Cached state
            with prefect.context(task_hash_name=self.task_full_name):
                state = super().check_target(state=state, inputs=inputs)
            return state

        return state

    def get_task_inputs(self, *args, **kwargs) -> Dict[str, Result]:
        task_inputs = super().get_task_inputs(*args, **kwargs)
        if self._should_track():
            lock = get_lock(self.task_full_name)
            lock["raw_inputs"] = task_inputs
            set_lock(self.task_full_name, lock)
        return task_inputs

    def _hash_source(self):
        source = inspect.getsource(self.task.run)
        # Strip task decorator
        if source.startswith("@task"):
            source = source.split("\n", maxsplit=1)[1]
        assert source.startswith(
            "def"
        ), f"Source for task {self.task.name} does not start with def, using decorator?"
        return self._hash_result(result=source, serializer=SourceSerializer())

    def _hash_inputs(self):
        raw_inputs = get_lock(self.task_full_name).pop("raw_inputs")
        return {
            key: self._hash_result(input.value, serializer=input.serializer)
            for key, input in raw_inputs.items()
        }

    def generate_task_lock(self, state: State):
        return {
            "inputs": self._hash_inputs(),
            "source": self._hash_source(),
            "result": self._hash_result(result=state.result, serializer=self.result.serializer),
        }

    def run(self, *args, **kwargs) -> State:
        with prefect.context(task_hash_name=self.task_full_name):
            state = super().run(*args, **kwargs)
        if isinstance(state, Success) and self._should_track():
            task_lock = self.generate_task_lock(state=state)
            set_lock(self.task_full_name, task_lock)
        return state


class SourceSerializer(Serializer):
    def serialize(self, value: Any) -> bytes:
        return value.encode()

    def deserialize(self, value: bytes) -> Any:
        return value.decode()
