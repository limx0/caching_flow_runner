import json
import pathlib
from functools import lru_cache
from typing import Dict, List, Tuple

import fsspec
from fsspec import AbstractFileSystem
from fsspec.utils import infer_storage_options


LOCK = {}


def set_lock(key, value):
    global LOCK
    LOCK[key] = value


def get_lock(key=None):
    global LOCK
    if key is None:
        return LOCK.copy()
    return LOCK.get(key, {})


def clear_lock():
    global LOCK
    LOCK = {}


def check_parent_exists(fs, path):
    parent = str(pathlib.Path(path).parent)
    if not fs.exists(parent):
        fs.mkdir(parent)


@lru_cache()
def get_fs(url, check_exists=True) -> Tuple[AbstractFileSystem, str]:
    options = infer_storage_options(urlpath=url)
    fs = fsspec.filesystem(options["protocol"])
    root = url.replace(options["protocol"] + "://", "")

    if check_exists:
        check_parent_exists(fs=fs, path=root)
    return fs, root


class LockStore:
    def __init__(self, url_path):
        self.fs, self.root = get_fs(url_path)

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
        with self.fs.open(f"{self.root}/{key}.json", "wb") as f:
            return f.write(json.dumps(data).encode())

    def load(self, key) -> Dict:
        try:
            with self.fs.open(f"{self.root}/{key}.json", "rb") as f:
                return json.loads(f.read())
        except FileNotFoundError:
            return {}
