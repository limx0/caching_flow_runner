import datetime
from typing import Dict

import prefect
from openlineage.client.run import InputDataset
from openlineage.client.run import OutputDataset
from prefect import Parameter
from prefect.engine import TaskRunner
from prefect.engine.result import Result
from prefect.engine.state import Failed
from prefect.engine.state import Pending
from prefect.engine.state import Running
from prefect.engine.state import State
from prefect.engine.state import Success

from caching_flow_runner.adapter import MarquezAdapter
from caching_flow_runner.util import task_qualified_name


class OpenLineageTaskRunner(TaskRunner):
    def __init__(self, *args, client: MarquezAdapter, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = client
        self.task_full_name = task_qualified_name(task=self.task)
        self.state_handlers.append(self.on_state_changed)

    def make_timestamp(self):
        return datetime.datetime.now(datetime.timezone.utc).isoformat()[:-6] + "Z"

    def on_state_changed(self, _, old_state: State, new_state: State):
        if isinstance(old_state, Running) and isinstance(new_state, Success):
            self._on_success(new_state)
        elif isinstance(old_state, (Pending, Running)) and isinstance(new_state, Failed):
            self._on_failure(state=new_state)

    def _parse_inputs(self, inputs: Dict[str, Result]):
        """Convert prefect inputs to input Datasets for OpenLineage"""
        return [InputDataset({k: v.value for k, v in inputs.items()})]

    def _parse_output(self, result):
        """Convert task result to output Dataset for OpenLineage"""
        return [OutputDataset(namespace="", name=self.task_full_name)]

    def _task_description(self):
        if isinstance(self.task, Parameter):
            # Parameters don't have any doc / description at this stage, simply return the name
            return self.task.name
        else:
            return self.task.__doc__

    def _on_start(self, inputs: Dict[str, Result]):
        context = prefect.context
        run_id = self._client.start_task(
            run_id=context.task_run_id,
            job_name=self.task_full_name,
            job_description=self._task_description(),
            event_time=self.make_timestamp(),
            parent_run_id=context.flow_run_id,
            code_location=None,
            inputs=self._parse_inputs(inputs),
            outputs=None,
        )
        self.logger.info(f"Marquez run CREATED run_id: {run_id}")

    def _on_success(self, state: Success):
        context = prefect.context
        run_id = self._client.complete_task(
            run_id=context.task_run_id,
            job_name=self.task_full_name,
            inputs=None,
            end_time=self.make_timestamp(),
            outputs=state.result,
        )
        self.logger.info(f"Marquez run COMPLETE run_id: {run_id}")

    def _on_failure(self, state: Failed):
        context = prefect.context
        run_id = self._client.fail_task(
            run_id=context.task_run_id,
            job_name=self.task_full_name,
            inputs=None,
            outputs=None,
            end_time=self.make_timestamp(),
        )
        self.logger.info(f"Marquez run FAILED run_id: {run_id}")

    def set_task_to_running(self, state: State, inputs: Dict[str, Result]) -> State:
        state = super().set_task_to_running(state=state, inputs=inputs)
        self._on_start(inputs=inputs)
        return state
