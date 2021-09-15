import os

from caching_flow_runner.flow_runner import CachedFlowRunner
from caching_flow_runner.test_utils.tasks import test_flow
from caching_flow_runner.util import get_fs


class TestCachedFlowRunner:
    def setup(self):
        self.fs_url = os.environ.get("FS_URL", "memory:///")
        self.flow = test_flow
        self.fs, self.root = get_fs(self.fs_url)
        self.runner_cls = CachedFlowRunner
        self._clear_fs()

    def _clear_fs(self):
        try:
            self.fs.rm(f"{self.root}", recursive=True)
        except FileNotFoundError:
            pass
        self.fs.mkdir(self.root)

    def _ls(self):
        return self.fs.glob(f"{self.root}/**/*")

    def test_flow_run(self):
        self.flow.run(p=1)
