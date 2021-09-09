from functools import lru_cache
from typing import Any

import fsspec
import prefect
from prefect.engine.result import Result
from prefect.engine.serializers import Serializer


@lru_cache(1)
def get_fs():
    return fsspec.filesystem("memory")


def task_filename(*args, **kwargs) -> str:
    pass


class MemoryResult(Result):
    def __init__(
        self,
        value: Any = None,
        location: str = None,
        serializer: Serializer = None,
    ):
        super().__init__(value=value, location=location, serializer=serializer)
        self.fs = get_fs()
        self.logger = prefect.context["logger"]

    def read(self, location: str) -> "Result":
        self.logger.info(f"reading from {location=}")
        new = self.copy()
        new.location = location

        with self.fs.open(location, "rb") as f:
            serialized = f.read()
        new.value = self.serializer.deserialize(serialized)
        self.logger.info(f"Got value {new.value=}")
        return new

    def write(self, value_: Any, **kwargs: Any) -> "Result":
        new = self.format(**kwargs)

        new.value = value_
        value = self.serializer.serialize(new.value)
        self.logger.info(f"Writing to cache location {new.location=} {new.value}")

        with self.fs.open(new.location, "wb") as f:
            f.write(value)

        return new

    def exists(self, location: str, **kwargs: Any) -> bool:
        if "task_hash_name" not in kwargs:
            return False
        path = location.format(**kwargs)
        exists = self.fs.exists(path)
        self.logger.info(f"Checking exists {path=} {exists=}")
        return exists
