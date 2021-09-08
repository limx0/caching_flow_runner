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
from prefect.utilities.executors import tail_recursive


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

    def _loop_task_name(self, message: str):
        return f"{self.task_full_name}-{message}"

    def _hash_source(self):
        """Hash source code for run function, stripping away @task decorator"""
        source = inspect.getsource(self.task.run)
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
        self.logger.info(f"Setting task lock for {self.task_full_name} based on {new_state}")
        task_lock = self._generate_task_lock(state=new_state)
        set_lock(self.task_full_name, task_lock)
        return new_state

    # def _on_looped(self, state: Looped):
    #     """ We've completed a LOOP task, persist the result and lock file """
    #     task_lock = self._generate_task_lock(state=state)
    #     set_lock(self._loop_task_name(message=state.message), task_lock)
    #     return state

    def _on_state_change(self, _, old_state: State, new_state: State) -> State:
        self.logger.info(f"{old_state=} {new_state=}")
        if self._should_track():
            if isinstance(new_state, Success):
                return self._on_success(new_state=new_state)
            elif isinstance(new_state, Looped):
                self.logger.info(f"Setting task_loop_message={new_state.message}")
                self.context.update(task_loop_message=new_state.message)

        return new_state

    def _persist_loop_result(self, state: Looped, result: Any):
        with prefect.context(task_hash_name=f"{self.task_full_name}-{state.message}"):
            self.logger.info(f"Writing loop result {result}")
            self.result.write(result, **prefect.context)

    def get_task_inputs(self, *args, **kwargs) -> Dict[str, Result]:
        task_inputs = super().get_task_inputs(*args, **kwargs)
        if self._should_track():
            lock = get_lock(self.task_full_name)
            lock["raw_inputs"] = task_inputs
            lock["loop_inputs"] = kwargs.get("state").result
            set_lock(self.task_full_name, lock)
        return task_inputs

    # def cache_result(self, state: State, inputs: Dict[str, Result]) -> State:
    #     result = super().cache_result(state=state, inputs=inputs)
    #     if isinstance(state, Looped):
    #         lock = self._generate_task_lock(state=state)
    #         set_lock(self._loop_task_name(message=state.message), lock)
    #         self._persist_loop_result(state=state, result=result.result)
    #     return result

    def check_target(self, state: State, inputs: Dict[str, Result]) -> State:
        """Overloaded purely to inject task_hash_name when reading from target"""
        if self.context.get("task_loop_message") is not None:
            fn = f"{self.task_full_name}-{self.context['task_loop_message']}"
            with prefect.context(task_hash_name=fn):
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

    @tail_recursive
    def run(self, *args, **kwargs) -> State:
        with prefect.context(task_hash_name=self.task_full_name):
            return super().run(*args, **kwargs)


class SourceSerializer(Serializer):
    def serialize(self, value: Any) -> bytes:
        return value.encode()

    def deserialize(self, value: bytes) -> Any:
        return value.decode()
