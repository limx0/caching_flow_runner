from functools import partial

import cloudpickle
import pytest
from prefect import Flow
from prefect import Parameter
from prefect.engine.state import Cached
from prefect.engine.state import Success

from caching_flow_runner.flow_runner import CachedFlowRunner
from caching_flow_runner.hash_storage import HashStorage
from caching_flow_runner.task_runner import clear_lock
from caching_flow_runner.task_runner import get_lock
from caching_flow_runner.test_utils.locks import task_lock_instance
from caching_flow_runner.test_utils.tasks import inc
from caching_flow_runner.test_utils.tasks import looping_task
from caching_flow_runner.test_utils.tasks import test_flow


class TestCachedFlowRunner:
    def setup(self):
        self.flow = test_flow
        self.hash_storage = HashStorage("memory://")
        self.runner_cls = partial(CachedFlowRunner, hash_storage=self.hash_storage)
        clear_lock()

    def _serialize_to_cache(self, fn, value):
        with self.hash_storage.fs.open(fn, "wb") as f:
            serialized = cloudpickle.dumps(value)
            f.write(serialized)

    def test_lock(self):
        # Arrange,
        expected = task_lock_instance

        # Act
        self.flow.run(p=1, runner_cls=self.runner_cls)

        # Assert
        assert get_lock() == expected

    def test_previous_flow_run_caches_correctly(self):
        # Arrange - pre cache data
        self.hash_storage.save_multiple(data=task_lock_instance.copy())
        self._serialize_to_cache("/caching_flow_runner.test_utils.tasks.get.pkl", 1)
        self._serialize_to_cache("/caching_flow_runner.test_utils.tasks.inc.pkl", 2)

        # Act
        states = self.flow.run(p=1, runner_cls=self.runner_cls)

        # Assert
        result = {task.name: state for task, state in states.result.items()}
        assert isinstance(result["get"], Cached)
        assert isinstance(result["inc"], Cached)

    def test_task_recomputes_when_when_hash_changes(self):
        # Arrange
        self.hash_storage.save_multiple(data=task_lock_instance.copy())
        self._serialize_to_cache("/caching_flow_runner.test_utils.tasks.get.pkl", 1)
        self._serialize_to_cache("/caching_flow_runner.test_utils.tasks.inc.pkl", 2)

        # Act
        inc_lock = self.hash_storage.load("caching_flow_runner.test_utils.tasks.inc")
        inc_lock["inputs"]["b"]["md5"] = "111"
        self.hash_storage.save(key="caching_flow_runner.test_utils.tasks.inc", values=inc_lock)

        states = self.flow.run(p=1, runner_cls=self.runner_cls)

        # Assert
        result = {task.name: state for task, state in states.result.items()}
        assert isinstance(result["get"], Cached)
        assert isinstance(result["inc"], Success)

    @pytest.mark.skip(reason="Not implemented")
    def test_mapping_task(self):
        raise NotImplementedError

    # @mock.patch("caching_flow_runner.test_utils.memory_result.MemoryResult.read")
    def test_looping_task(self):
        # Arrange
        with Flow("loop_flow") as flow:
            n = Parameter("n")
            loop = looping_task(n=n)
            inc(loop)

        flow.run(n=3, runner_cls=self.runner_cls)

        # Act
        flow.run(n=5, runner_cls=self.runner_cls)

        # Assert
