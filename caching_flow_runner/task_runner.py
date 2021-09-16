import datetime
from typing import Dict

import prefect
from prefect import Parameter
from prefect.engine import TaskRunner
from prefect.engine.result import Result
from prefect.engine.state import State

from caching_flow_runner.adapter import OpenLineageAdapter
from caching_flow_runner.util import task_qualified_name


class OpenLineageTaskRunner(TaskRunner):
    def __init__(self, *args, lineage_adapter: OpenLineageAdapter, **kwargs):
        super().__init__(*args, **kwargs)
        self._adapter = lineage_adapter
        self.task_full_name = task_qualified_name(task=self.task)

    def unpack_inputs(self, inputs: Dict[str, Result]):
        """Unpack values from Result"""
        return {k: v.value for k, v in inputs.items()}

    def _task_description(self):
        if isinstance(self.task, Parameter):
            # Parameters don't have any doc / description at this stage, simply return the name
            return self.task.name
        else:
            return self.task.__doc__

    def _on_start(self, inputs: Dict[str, Result]):
        # TODO - Should we skip parameters?
        context = prefect.context
        self._adapter.start_task(
            run_id=context.task_run_id,
            job_name=self.task_full_name,
            job_description=self._task_description(),
            event_time=datetime.datetime.utcnow().isoformat(),
            parent_run_id=context.flow_run_id,
            code_location=None,
            inputs=self.unpack_inputs(inputs),
            outputs=None,
        )

    def set_task_to_running(self, state: State, inputs: Dict[str, Result]) -> State:
        state = super().set_task_to_running(state=state, inputs=inputs)
        self._on_start(inputs=inputs)
        return state
