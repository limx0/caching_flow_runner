from prefect.engine import FlowRunner
from prefect.engine.state import State

from caching_flow_runner.task_runner import CachedTaskRunner


class CachedFlowRunner(FlowRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, task_runner_cls=CachedTaskRunner, **kwargs)

    def run(self, *args, **kwargs):
        """
        Because parameters are not injected until `run`, we need to overload this method to perform optimisation
        """
        return super().run(*args, **kwargs)

    def get_flow_run_state(self, *args, **kwargs) -> State:
        state = super().get_flow_run_state(*args, **kwargs)
        return state
