# caching_flow_runner

An attempt to implement some simple data lineage for prefect flows. 

Uses a `CachedFlowRunner` and `CachedTaskRunner` to 
record input and output hashes for all tasks that run, overriding `TaskRunner.check_target` to also check for matching
hashes.

### To do:
- Persist flow lock for faster repeated runs
- Implement skip loading upstream from cache if a task is cache valid.
