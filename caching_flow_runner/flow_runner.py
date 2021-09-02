from typing import Dict, Optional

from prefect.engine import FlowRunner
from prefect.engine.state import State

from caching_flow_runner.hash_storage import HashStorage
from caching_flow_runner.task_runner import CachedTaskRunner
from caching_flow_runner.task_runner import task_qualified_name


class CachedFlowRunner(FlowRunner):
    def __init__(self, *args, hash_storage: HashStorage, **kwargs):
        super().__init__(*args, task_runner_cls=CachedTaskRunner, **kwargs)
        self.hash_storage = hash_storage

    def determine_initial_task_states(self, task_states: Optional[Dict]):
        for task in self.flow.sorted_tasks():
            lock = self.hash_storage.load(key=task_qualified_name(task))
            if not lock:
                continue
            print(lock)

        return task_states

    def save_lock(self):
        lock = self.task_runner_cls.LOCK
        self.hash_storage.save_multiple(data=lock)

    def get_flow_run_state(self, *args, **kwargs) -> State:
        self.determine_initial_task_states(task_states=kwargs.get("task_states"))
        # kwargs["task_states"] = cached_task_states
        state = super().get_flow_run_state(*args, **kwargs)
        self.save_lock()
        return state
