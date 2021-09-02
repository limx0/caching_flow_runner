from functools import partial

from caching_flow_runner.flow_runner import CachedFlowRunner
from caching_flow_runner.hash_storage import HashStorage
from caching_flow_runner.task_runner import CachedTaskRunner
from caching_flow_runner.test_utils.tasks import flow


class TestCachedFlowRunner:
    def setup(self):
        self.flow = flow
        self.runner_cls = partial(CachedFlowRunner, hash_storage=HashStorage("memory://"))

    def test_lock(self):
        self.flow.run(p=1, runner_cls=self.runner_cls)
        expected = {
            "caching_flow_runner.test_utils.tasks.get": {
                "inputs": {"a": {"md5": "c4ca4238a0b923820dcc509a6f75849b", "size": 1}},
                "result": {"md5": "1d847da32ecaabf6731c38f798c3d4ce", "size": 5},
                "source": {"md5": "a38a2f18eac2f848a0cfbe764f391d1d", "size": 112},
            },
            "caching_flow_runner.test_utils.tasks.inc": {
                "inputs": {"b": {"md5": "1d847da32ecaabf6731c38f798c3d4ce", "size": 5}},
                "result": {"md5": "a9ec4f5f33f0d64e74ed5d9900bceac6", "size": 5},
                "source": {"md5": "ad62a21339857e9ebfba1a9291ba1991", "size": 116},
            },
            "caching_flow_runner.test_utils.tasks.multiply": {
                "inputs": {"c": {"md5": "a9ec4f5f33f0d64e74ed5d9900bceac6", "size": 5}},
                "result": {"md5": "0a29745a784ff38107fdb40e0b199fdf", "size": 5},
                "source": {"md5": "3d2eb34cd69c8dad02bc71462f2f9c12", "size": 57},
            },
        }
        assert CachedTaskRunner.LOCK == expected

    def test_rerun_states(self):
        # Cache
        self.flow.run(p=1, runner_cls=self.runner_cls)

        self.runner_cls.LOCK = {}


        self.flow.run(p=1, runner_cls=self.runner_cls)
