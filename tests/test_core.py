import os
import time

from marquez_client import MarquezClient

from caching_flow_runner.flow_runner import OpenLineageFlowRunner
from caching_flow_runner.test_utils.tasks import test_flow
from caching_flow_runner.util import get_fs


# create namespace

pytest_plugins = ["docker_compose"]


class TestCachedFlowRunner:
    def setup(self):
        self.fs_url = os.environ.get("FS_URL", "memory:///")
        self.flow = test_flow
        self._setup_open_lineage()
        self.fs, self.root = get_fs(self.fs_url)
        self._clear_fs()
        self.runner_cls = OpenLineageFlowRunner
        self.client = MarquezClient(url="http://localhost:5000")

    def _clear_fs(self):
        try:
            self.fs.rm(f"{self.root}", recursive=True)
        except FileNotFoundError:
            pass
        self.fs.mkdir(self.root)

    def _ls(self):
        return self.fs.glob(f"{self.root}/**/*")

    def _setup_open_lineage(self):
        # Can't seem to delete from open lineage, so create a fresh namespace for each test run
        os.environ["OPENLINEAGE_NAMESPACE"] = str(time.time())
        os.environ["MARQUEZ_URL"] = "localhost:3000"

    def test_flow_run(self):
        self.flow.run(p=1, runner_cls=self.runner_cls)

    def test_dataset(self):
        pass
