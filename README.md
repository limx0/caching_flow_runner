# caching_flow_runner

An attempt to implement some simple data lineage for prefect flows. 

Uses a `CachedFlowRunner` and `CachedTaskRunner` to 
record input and output hashes for all tasks that run, overriding `TaskRunner.check_target` to also check for matching
hashes.

### Install
```shell
pip install git+https://github.com/limx0/caching_flow_runner.git
```

### Usage
```python
from caching_flow_runner.flow_runner import CachedFlowRunner

# Create flow as normal
flow = MyFlow(...) 

# Run with CachedFlowRunner
flow.run(runner_cls=CachedFlowRunner)
```

### To do:
- [ ] Test mapping tasks
- [ ] Test looping tasks
- [ ] Implement flow trimming when tasks are cached
