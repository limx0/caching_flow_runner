# caching_flow_runner

An attempt to implement some simple data lineage for prefect flows (Very WIP). 

Uses a `CachedFlowRunner` and `CachedTaskRunner` to 
record input and output hashes for all tasks that run, overriding `TaskRunner.check_target` to also check for matching
hashes.

### Why?

Some background can be found at [Prefect #4935](https://github.com/PrefectHQ/prefect/discussions/4935). I wanted to see 
what we could possibly implement in a short time, as well as get more familiar with Prefects task/flow running code. It 
could also serve as a reference or point of discussion if Prefect were to ever implement this in the core code base.

### Install
```shell
pip install git+https://github.com/limx0/caching_flow_runner.git
```
### Usage
```python
from functools import partial
from caching_flow_runner.flow_runner import CachedFlowRunner
from caching_flow_runner.lock_storage import LockStore

# Create flow as normal
flow = MyFlow(...) 

# Create a LockStore to persist lock data
store = LockStore("memory://") # or LockStore("/path/to/directory")

# Run with CachedFlowRunner
flow.run(runner_cls=partial(CachedFlowRunner, lock_store=store))
```

### To do:
- [ ] Test mapping tasks
- [x] Test looping tasks
  - this basically works, although you would probably want better state being passed around to generate the cache locations per loop. 
- [ ] Implement flow trimming when tasks are cached

