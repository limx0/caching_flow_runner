import os
from functools import partial

import cloudpickle
import pytest
from prefect import Flow
from prefect import Parameter
from prefect.engine.state import Cached
from prefect.engine.state import Success

from caching_flow_runner.flow_runner import CachedFlowRunner
from caching_flow_runner.lock_storage import LockStore
from caching_flow_runner.lock_storage import check_parent_exists
from caching_flow_runner.lock_storage import clear_lock
from caching_flow_runner.lock_storage import set_lock
from caching_flow_runner.task_runner import get_lock
from caching_flow_runner.test_utils.locks import task_lock_instance
from caching_flow_runner.test_utils.memory_result import get_fs
from caching_flow_runner.test_utils.tasks import get
from caching_flow_runner.test_utils.tasks import inc
from caching_flow_runner.test_utils.tasks import looping_task
from caching_flow_runner.test_utils.tasks import test_flow


class TestCachedFlowRunner:
    def setup(self):
        self.fs_url = os.environ.get("FS_URL", "memory:///")
        self.flow = test_flow
        self.fs, self.root = get_fs(self.fs_url)
        self.lock_store = LockStore(self.fs_url)
        self.runner_cls = partial(CachedFlowRunner, lock_store=self.lock_store)
        self._clear_fs()
        clear_lock()

    def _clear_fs(self):
        try:
            self.fs.rm(f"{self.root}", recursive=True)
        except FileNotFoundError:
            pass
        self.fs.mkdir(self.root)

    def _ls(self):
        return self.fs.glob(f"{self.root}/**/*")

    def _serialize_to_cache(self, path, value):
        fn = f"{self.root}{path}"
        check_parent_exists(fs=self.fs, path=fn)
        with self.fs.open(fn, "wb") as f:
            serialized = cloudpickle.dumps(value)
            f.write(serialized)

    def test_lock(self):
        # Arrange,
        expected = task_lock_instance

        # Act
        self.flow.run(p=1, runner_cls=self.runner_cls)

        # Assert
        assert get_lock() == expected

    @pytest.mark.local
    def test_writing_to_cache_produces_hashed_filename(self):
        # Arrange
        with Flow("test") as flow:
            get(1)

        # Act
        flow.run(runner_cls=self.runner_cls)

        # Assert
        result = self._ls()
        expected = [
            f"{self.root}caching_flow_runner.test_utils.tasks.get/2c671b46cc1b6b790da36e361ccaecf8.pkl",
        ]
        assert result == expected

    @pytest.mark.local
    def test_different_parameters_dont_collide(self):
        # Arrange
        with Flow("test") as flow:
            get(1)
            get(2)

        # Act
        flow.run(runner_cls=self.runner_cls)

        # Assert
        result = self._ls()
        expected = [
            f"{self.root}caching_flow_runner.test_utils.tasks.get/2c671b46cc1b6b790da36e361ccaecf8.pkl",
            f"{self.root}caching_flow_runner.test_utils.tasks.get/45ba0c8bb03803bcb166da81070e775f.pkl",
        ]
        assert result == expected

    def test_previous_flow_run_caches_correctly(self):
        # Arrange - pre cache data
        self.lock_store.save_multiple(data=task_lock_instance.copy())
        self._serialize_to_cache(
            "caching_flow_runner.test_utils.tasks.get/2c671b46cc1b6b790da36e361ccaecf8.pkl", 1
        )
        self._serialize_to_cache(
            "caching_flow_runner.test_utils.tasks.inc/45e8aaaf26a60b0a847fb7331a0e02aa.pkl", 2
        )

        # Act
        states = self.flow.run(p=1, runner_cls=self.runner_cls)

        # Assert
        result = {task.name: state for task, state in states.result.items()}
        assert isinstance(result["get"], Cached)
        assert isinstance(result["inc"], Cached)

    def test_task_recomputes_when_when_hash_changes(self):
        # Arrange
        self.lock_store.save_multiple(data=task_lock_instance.copy())
        self._serialize_to_cache(
            "caching_flow_runner.test_utils.tasks.get/2c671b46cc1b6b790da36e361ccaecf8.pkl", 1
        )
        self._serialize_to_cache(
            "caching_flow_runner.test_utils.tasks.inc/45e8aaaf26a60b0a847fb7331a0e02aa.pkl", 2
        )

        # Act
        inc_lock = self.lock_store.load("caching_flow_runner.test_utils.tasks.inc")
        inc_lock["inputs"]["b"]["md5"] = "111"
        self.lock_store.save(key="caching_flow_runner.test_utils.tasks.inc", values=inc_lock)

        states = self.flow.run(p=1, runner_cls=self.runner_cls)

        # Assert
        result = {task.name: state for task, state in states.result.items()}
        assert isinstance(result["get"], Cached)
        assert isinstance(result["inc"], Success)

    @pytest.mark.skip(reason="Not implemented")
    def test_mapping_task(self):
        raise NotImplementedError

    def test_looping_task(self):
        # Arrange
        with Flow("loop_flow") as flow:
            n = Parameter("n")
            loop = looping_task(n=n)
            inc(loop)

        flow.run(n=3, runner_cls=self.runner_cls)

        # Act

        flow.logger.info("\n\n")

        flow.run(n=3, runner_cls=self.runner_cls)

        # Assert

    # def test_flow_runner_no_cache(self):
    #     # Act
    #     self.flow.run(p=1, runner_cls=self.runner_cls)

    def test_flow_runner_cached(self):
        # Arrange
        for key, value in task_lock_instance.items():
            if key.endswith("multiply"):
                continue
            set_lock(key, value)
        self._serialize_to_cache(
            f"{self.root}caching_flow_runner.test_utils.tasks.get/2c671b46cc1b6b790da36e361ccaecf8.pkl",
            1,
        )
        self._serialize_to_cache(
            f"{self.root}caching_flow_runner.test_utils.tasks.inc/45e8aaaf26a60b0a847fb7331a0e02aa.pkl",
            2,
        )

        # Act
        runner = self.runner_cls(flow=self.flow)
        flow = runner.optimise_flow(flow=runner.flow, parameters={"p": 1})

        # Assert
        task_names = {t.name for t in flow.tasks}
        assert task_names == {"multiply", "inc"}
        edges = {(edge.upstream_task.name, edge.downstream_task.name) for edge in flow.edges}
        assert edges == {("inc", "multiply")}
