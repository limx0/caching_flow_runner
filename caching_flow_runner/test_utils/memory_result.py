from typing import Any

import fsspec
from prefect.engine.result import Result
from prefect.engine.serializers import Serializer


class MemoryResult(Result):
    def __init__(
        self,
        value: Any = None,
        location: str = None,
        serializer: Serializer = None,
    ):
        super().__init__(value=value, location=location, serializer=serializer)
        self.fs = fsspec.filesystem("memory")

    def read(self, location: str) -> "Result":
        new = self.copy()
        new.location = location

        with self.fs.open(location, "rb") as f:
            serialized = f.read()
        new.value = self.serializer.deserialize(serialized)
        return new

    def write(self, value_: Any, **kwargs: Any) -> "Result":
        new = self.format(**kwargs)
        new.value = value_
        value = self.serializer.serialize(new.value)

        with self.fs.open(new.location, "wb") as f:
            f.write(value)

        return new

    def exists(self, location: str, **kwargs: Any) -> bool:
        if "task_hash_name" not in kwargs:
            return False
        return self.fs.exists(location.format(**kwargs))
