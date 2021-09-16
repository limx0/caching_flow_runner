# FlowRunner

Prototyping using open lineage alongside Prefect via a `FlowRunner` and `TaskRunner` subclass.

See:
- [Adapter](https://github.com/limx0/caching_flow_runner/blob/open_lineage/caching_flow_runner/adapter.py) - clone of the airflow implementation at this stage
- [FlowRunner](https://github.com/limx0/caching_flow_runner/blob/open_lineage/caching_flow_runner/flow_runner.py) - Sets up the TaskRunner 
- [TaskRunner](https://github.com/limx0/caching_flow_runner/blob/open_lineage/caching_flow_runner/task_runner.py) - Maps actual task state changes to open lineage events 
