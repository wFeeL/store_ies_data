try:
    import orjson as json
except ImportError:
    import json
import sys
import urllib.request
from urllib.error import URLError
from .structures import *

__version__ = "1.0.2026.1"
__all__ = ["init", "init_test", "from_json", "from_file", "from_log", "Powerstand"]


def get_library_path():
    import os
    path = os.path.abspath(__file__)
    return os.path.dirname(path)


def init() -> Powerstand:
    try:
        request = urllib.request.urlopen("http://localhost:26000/powerstand")
        if request.getcode() != 200:
            raise ConnectionRefusedError("Couldn't receive data from server")
        data = json.loads(request.read())
        return Powerstand(data, offline=False)
    except URLError as e:
        print(e, file=sys.stderr)
        print("<<< СЕРВЕР НЕДОСТУПЕН, ИСПОЛЬЗУЙТЕ ЛОКАЛЬНЫЙ РЕЖИМ. >>>", file=sys.stderr)
        exit(1)


def init_test() -> Powerstand:
    from .test import stub_input
    return from_json(stub_input)


def from_json(string) -> Powerstand:
    data = json.loads(string)
    return Powerstand(data)


def from_file(filename) -> Powerstand:
    with open(filename, "r") as fin:
        raw_data = fin.read()
    return from_json(raw_data)
    
def from_log(filename, step) -> Powerstand:
    with open(filename, "rb") as fin:
        raw_data = json.loads(fin.read()) 
    ps = raw_data[step]['powerstand']['contents']['state']
    if ps is None:
        raise ValueError("состояние ещё пустое")
    tag = raw_data[step]['powerstand']["tag"]
    if tag.startswith("VariantState_"):
        tag = "Core" + tag[13:]
    data = {
        "tag": tag,
        "data": ps
    }
    return Powerstand(data)

