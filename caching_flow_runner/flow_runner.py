from typing import Any, Dict

import prefect
from prefect import Flow
from prefect import Parameter
from prefect.engine import FlowRunner
from prefect.engine.state import State

from caching_flow_runner.lock_storage import LockStore
from caching_flow_runner.task_runner import CachedTaskRunner
from caching_flow_runner.task_runner import _hash_result
from caching_flow_runner.task_runner import get_lock
from caching_flow_runner.task_runner import set_lock
from caching_flow_runner.task_runner import task_qualified_name


class CachedFlowRunner(FlowRunner):
    def __init__(self, *args, lock_store: LockStore, optimise_flow=False, **kwargs):
        super().__init__(*args, task_runner_cls=CachedTaskRunner, **kwargs)
        self.lock_store = lock_store
        self._optimise_flow = optimise_flow

    @staticmethod
    def optimise_flow(flow: Flow, parameters: Dict[str, Any] = None):  # noqa: C901
        """
        Walk the graph in this flow from the roots, substituting parameters and determining (based on the lock file)
        which tasks can safely be dropped from computation/loading from the cache.
        """
        with prefect.context(parameters=parameters):
            # Determine which tasks are cached
            state = {}
            for task in flow.sorted_tasks():
                skip = False
                name = task_qualified_name(task=task)
                lock = get_lock(key=name)

                # Check all upstream edges exist in the state
                for upstream in [edge.upstream_task for edge in flow.edges_to(task)]:
                    if upstream not in state:
                        skip = True

                if skip:
                    continue

                # Check inputs against cached data
                for edge in flow.edges_to(task):
                    inputs = state[edge.upstream_task]
                    key = edge.key
                    if lock.get("inputs", {}).get(key) != inputs:
                        skip = True

                if skip:
                    continue

                # At this point - the task
                if isinstance(task, Parameter):
                    state[task] = _hash_result(task.run(), serializer=task.result.serializer)
                else:
                    state[task] = lock["result"]

            # Determine which tasks we can safely drop. We have to walk the graph twice AFAICT because I think we're
            # doing a depth first walk.
            tasks_to_drop = set()
            edges_to_drop = set()
            for task in flow.sorted_tasks():
                all_upstreams_valid = (
                    all([upstream in state for upstream in flow.edges_to(task)]) or True
                )  # if empty
                task_valid = task in state
                if all_upstreams_valid and task_valid:
                    tasks_to_drop.add(task)
                    edges_to_drop.update(flow.edges_from(task))
                else:
                    # We need to remove any upstreams of this task from being dropped because they will be the inputs
                    for edge in flow.edges_to(task):
                        if edge.upstream_task in tasks_to_drop:
                            tasks_to_drop.remove(edge.upstream_task)
                            edges_to_drop.remove(edge)

            # Finally, drop the tasks & edges from the flow
            for edge in edges_to_drop:
                flow.edges.remove(edge)
            for task in tasks_to_drop:
                flow.tasks.remove(task)

            return flow

    def run(self, *args, **kwargs):
        """
        Because parameters are not injected until `run`, we need to overload this method to perform optimisation
        """
        if self._optimise_flow:
            self.flow = self.optimise_flow(
                flow=self.flow.copy(), parameters=kwargs.get("parameters")
            )
        return super().run(*args, **kwargs)

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
