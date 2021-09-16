from prefect.engine import FlowRunner
from prefect.engine.state import State

from caching_flow_runner.adapter import OpenLineageAdapter
from caching_flow_runner.task_runner import OpenLineageTaskRunner


class OpenLineageFlowRunner(FlowRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, task_runner_cls=OpenLineageTaskRunner, **kwargs)
        self._adapter = OpenLineageAdapter()

    def run(self, *args, **kwargs):
        return super().run(*args, **kwargs)

    def get_flow_run_state(self, *args, **kwargs) -> State:
        state = super().get_flow_run_state(*args, **kwargs)
        return state
