from prefect import Parameter
from prefect.engine import FlowRunner
from prefect.engine.state import State

from caching_flow_runner.lock_storage import LockStore
from caching_flow_runner.task_runner import CachedTaskRunner
from caching_flow_runner.task_runner import get_lock
from caching_flow_runner.task_runner import set_lock
from caching_flow_runner.task_runner import task_qualified_name


class CachedFlowRunner(FlowRunner):
    def __init__(self, *args, lock_store: LockStore, **kwargs):
        super().__init__(*args, task_runner_cls=CachedTaskRunner, **kwargs)
        self.lock_store = lock_store

    def set_locks_for_flow_run(self):
        for task in self.flow.sorted_tasks():
            name = task_qualified_name(task)
            lock = self.lock_store.load(key=name)
            if not isinstance(task, Parameter):
                set_lock(name, lock)

    def record_locks_post_run(self):
        lock = get_lock()
        self.lock_store.save_multiple(data=lock)

    def get_flow_run_state(self, *args, **kwargs) -> State:
        self.set_locks_for_flow_run()
        state = super().get_flow_run_state(*args, **kwargs)
        self.record_locks_post_run()
        return state
