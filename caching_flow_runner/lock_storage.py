import json
from typing import Dict, List

import fsspec
from fsspec.utils import infer_storage_options


class LockStore:
    def __init__(self, url_path):
        self.url_path = url_path
        self.options = infer_storage_options(urlpath=url_path)
        self.fs = fsspec.filesystem(self.options["protocol"])

    def merge(self, key: str, values: Dict) -> Dict:
        merged = self.load(key=key)
        merged.update(values)
        return merged

    def save_multiple(self, data: Dict):
        for key, values in data.items():
            self.save(key=key, values=values)

    def load_multiple(self, keys: List[str]) -> Dict:
        data = {}
        for key in keys:
            data[key] = self.load(key=key)
        return data

    def save(self, key, values):
        data = self.merge(key, values)
        with self.fs.open(f"{self.url_path}/{key}.json", "wb") as f:
            return f.write(json.dumps(data).encode())

    def load(self, key) -> Dict:
        try:
            with self.fs.open(f"{self.url_path}/{key}.json", "rb") as f:
                return json.loads(f.read())
        except FileNotFoundError:
            return {}
