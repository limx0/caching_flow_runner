import hashlib
import inspect
from typing import Any, Dict

import prefect
from prefect import Parameter
from prefect import Task
from prefect.engine import TaskRunner
from prefect.engine.result import Result
from prefect.engine.serializers import Serializer
from prefect.engine.state import Cached
from prefect.engine.state import Looped
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


def _hash_result(result: Any, serializer: Serializer) -> Dict:
    serialized = serializer.serialize(result)
    hash = hashlib.md5(serialized).hexdigest()  # noqa: S303
    return {"md5": hash, "size": len(serialized)}


def _compare_input_hashes(inputs: Dict, lock: Dict):
    for key, result in inputs.items():
        if key not in lock:
            return False
        input_hash = _hash_result(result=result.value, serializer=result.serializer)
        lock_hash = lock[key]
        if input_hash != lock_hash:
            return False
    return True


class CachedTaskRunner(TaskRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_full_name = task_qualified_name(task=self.task)
        self.state_handlers.append(self._on_state_change)

    def _should_track(self):
        return not isinstance(self.task, Parameter)

    def _hash_source(self):
        source = inspect.getsource(self.task.run)
        # Strip task decorator
        if source.startswith("@task"):
            source = source.split("\n", maxsplit=1)[1]
        assert source.startswith(
            "def"
        ), f"Source for task {self.task.name} does not start with def, using decorator?"
        return _hash_result(result=source, serializer=SourceSerializer())

    def _hash_inputs(self):
        lock = get_lock(self.task_full_name)
        raw_inputs = lock.pop("raw_inputs")
        return {
            key: _hash_result(input.value, serializer=input.serializer)
            for key, input in raw_inputs.items()
        }

    def _hash_result(self, result):
        return _hash_result(result=result, serializer=self.result.serializer)

    def _generate_task_lock(self, state: State):
        return {
            "inputs": self._hash_inputs(),
            "source": self._hash_source(),
            "result": self._hash_result(result=state.result),
        }

    def _on_success(self, new_state):
        task_lock = self._generate_task_lock(state=new_state)
        set_lock(self.task_full_name, task_lock)
        return new_state

    def _on_looped(self, state: Looped):
        self.context["task_loop_message"] = state.message
        return state

    def _on_state_change(self, _, old_state: State, new_state: State) -> State:
        if self._should_track():
            if isinstance(new_state, Success):
                return self._on_success(new_state=new_state)
            elif isinstance(new_state, Looped):
                return self._on_looped(state=new_state)

        return new_state

    def get_task_inputs(self, *args, **kwargs) -> Dict[str, Result]:
        task_inputs = super().get_task_inputs(*args, **kwargs)
        if self._should_track():
            lock = get_lock(self.task_full_name)
            lock["raw_inputs"] = task_inputs
            lock["loop_inputs"] = kwargs.get("state").result
            set_lock(self.task_full_name, lock)
        return task_inputs

    def cache_result(self, state: State, inputs: Dict[str, Result]) -> State:
        result = super().cache_result(state=state, inputs=inputs)
        if isinstance(state, Looped):
            lock = self._generate_task_lock(state=state)
            set_lock(f"{self.task_full_name}-{state.message}", lock)
        return result

    def check_target(self, state: State, inputs: Dict[str, Result]) -> State:
        """Overloaded purely to inject task_hash_name when reading from target"""
        if self.context.get("task_loop_message") is not None:
            with prefect.context(
                task_hash_name=f"{self.task_full_name}-{self.context['task_loop_message']}"
            ):
                return super().check_target(state=state, inputs=inputs)

        with prefect.context(task_hash_name=self.task_full_name):
            return super().check_target(state=state, inputs=inputs)

    def check_task_is_cached(self, state: State, inputs: Dict[str, Result]) -> State:
        new_state = super().check_task_is_cached(state=state, inputs=inputs)

        # Additional check: result exists, need to check against lock
        if isinstance(state, Cached):
            lock = get_lock(self.task_full_name).get("inputs", {})
            if not _compare_input_hashes(inputs=inputs, lock=lock):
                return state

        return new_state


class SourceSerializer(Serializer):
    def serialize(self, value: Any) -> bytes:
        return value.encode()

    def deserialize(self, value: bytes) -> Any:
        return value.decode()
