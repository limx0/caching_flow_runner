import hashlib
import inspect
from typing import Dict

from prefect import Parameter, Task
from prefect.engine import TaskRunner
from prefect.engine.result import Result
from prefect.engine.state import State
from prefect.engine.state import Success


def task_qualified_name(task: Task):
    return f"{task.__module__}.{task.name}"


class CachedTaskRunner(TaskRunner):
    LOCK = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_full_name = task_qualified_name(task=self.task)
        if self.should_track():
            self.LOCK[self.task_full_name] = {}

    def should_track(self):
        return not isinstance(self.task, Parameter)

    def _hash_result(self, result: Result):
        if not isinstance(result, Result):
            result = Result(value=result)
        serializer = getattr(result, "serializer", self.result.serializer)
        serialized = serializer.serialize(result.value)
        hash = hashlib.md5(serialized).hexdigest()
        return {
            "md5": hash,
            "size": len(serialized),
        }

    def check_target(self, state: State, inputs: Dict[str, Result]) -> State:
        state = super().check_target(state=state, inputs=inputs)

        return state

    def check_task_is_cached(self, state: State, inputs: Dict[str, Result]) -> State:
        state = super().check_task_is_cached(state=state, inputs=inputs)
        return state

    def get_task_inputs(self, *args, **kwargs) -> Dict[str, Result]:
        task_inputs = super().get_task_inputs(*args, **kwargs)
        if self.should_track():
            self.LOCK[self.task_full_name]["inputs"] = task_inputs
        return task_inputs

    def source_hash(self):
        source = inspect.getsource(self.task.run)
        return self._hash_result(result=source)

    def generate_task_lock(self, state: State):
        inputs = self.LOCK[self.task_full_name].pop("inputs")
        return {
            self.task_full_name: {
                "inputs": {key: self._hash_result(result) for key, result in inputs.items()},
                "source": self.source_hash(),
                "result": self._hash_result(result=state.result),
            }
        }

    def run(self, *args, **kwargs) -> State:
        state = super().run(*args, **kwargs)
        if isinstance(state, Success) and self.should_track():
            self.LOCK.update(self.generate_task_lock(state=state))
        return state
