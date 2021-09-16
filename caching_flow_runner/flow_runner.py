from functools import partial

from prefect.engine import FlowRunner
from prefect.engine.state import State

from caching_flow_runner.adapter import OpenLineageAdapter
from caching_flow_runner.task_runner import OpenLineageTaskRunner


# TODO - FlowRunFacet? Store flow related data into a facet


# class OpenLineageStateHandler:
#     def __init__(self):
#         self._adapter = OpenLineageAdapter()
#
#     def _context(self) -> Context:
#         return prefect.context
#
#     def _on_start(self, task: Task):
#         context = self._context()
#         self._adapter.start_task(
#             run_id=context.task_run_id,
#             job_name=task_qualified_name(task),
#             job_description=task.__doc__,
#             event_time=datetime.datetime.utcnow().isoformat(),
#             parent_run_id=context.flow_run_id,
#             inputs=None
#         )
#
#     def __call__(self, task: Task, old_state: State, new_state: State):
#         task_full_name = task_qualified_name(task=task)
#         if isinstance(old_state, Pending) and isinstance(new_state, Running):
#             self._on_start(task=task)


class OpenLineageFlowRunner(FlowRunner):
    def __init__(self, *args, **kwargs):
        self._adapter = OpenLineageAdapter()
        task_runner_cls = partial(OpenLineageTaskRunner, lineage_adapter=self._adapter)
        super().__init__(*args, task_runner_cls=task_runner_cls, **kwargs)

    def run(self, *args, **kwargs):
        return super().run(*args, **kwargs)

    def get_flow_run_state(self, *args, **kwargs) -> State:
        state = super().get_flow_run_state(*args, **kwargs)
        return state
