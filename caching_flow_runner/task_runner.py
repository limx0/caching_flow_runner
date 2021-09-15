from prefect.engine import TaskRunner

from caching_flow_runner.util import task_qualified_name


class CachedTaskRunner(TaskRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_full_name = task_qualified_name(task=self.task)
