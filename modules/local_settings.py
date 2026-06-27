import json
import os
import threading
from typing import Any, Dict


_LOCK = threading.Lock()


def get_settings_file_path() -> str:
    env_path = os.getenv("METIS_LOCAL_SETTINGS_FILE", "").strip()
    if env_path:
        return os.path.abspath(env_path)

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(base_dir, "local_settings.json")


def _load_settings() -> Dict[str, Any]:
    path = get_settings_file_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}
    except OSError:
        return {}
    return {}


def _save_settings(data: Dict[str, Any]) -> None:
    path = get_settings_file_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp_path, path)


def get_setting(key: str, default: Any = None) -> Any:
    with _LOCK:
        settings = _load_settings()
    return settings.get(key, default)


def set_setting(key: str, value: Any) -> None:
    with _LOCK:
        settings = _load_settings()
        settings[key] = value
        _save_settings(settings)
