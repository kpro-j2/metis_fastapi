import os
from functools import lru_cache

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from modules.local_settings import get_setting, set_setting


DEFAULT_POSTGRES_URL = "postgresql+psycopg://metis:metis@localhost:5432/metis_state_log"
DEFAULT_SQLITE_PATH = os.path.abspath(
   os.path.join(os.path.dirname(__file__), "..", "nestdaq_state_log.db")
)
STATE_LOG_DB_CONFIG_KEY = "nestdaq_state_log_db_config"


class Base(DeclarativeBase):
   pass


EXPECTED_TABLES = (
   "alembic_version",
   "state_transitions",
   "anomaly_events",
   "runs",
)


def _state_log_defaults() -> dict:
   backend = str(os.getenv("METIS_STATE_LOG_DB_BACKEND", "postgresql")).strip().lower()
   if backend not in {"postgresql", "sqlite"}:
      backend = "postgresql"
   return {
      "backend": backend,
      "database_url": str(os.getenv("METIS_STATE_LOG_DATABASE_URL", "")).strip(),
      "postgres_url": str(os.getenv("METIS_STATE_LOG_POSTGRES_URL", DEFAULT_POSTGRES_URL)).strip(),
      "sqlite_path": str(os.getenv("METIS_STATE_LOG_SQLITE_PATH", DEFAULT_SQLITE_PATH)).strip(),
   }


def _normalize_state_log_config(config: dict) -> dict:
   defaults = _state_log_defaults()
   if not isinstance(config, dict):
      config = {}

   merged = dict(defaults)
   merged.update(config)

   backend = str(merged.get("backend", defaults["backend"]))
   backend = backend.strip().lower()
   if backend not in {"postgresql", "sqlite"}:
      backend = "postgresql"

   database_url = str(merged.get("database_url", "") or "").strip()
   postgres_url = str(merged.get("postgres_url", defaults["postgres_url"]) or "").strip()
   if postgres_url == "":
      postgres_url = defaults["postgres_url"]

   sqlite_path = str(merged.get("sqlite_path", defaults["sqlite_path"]) or "").strip()
   if sqlite_path == "":
      sqlite_path = defaults["sqlite_path"]

   return {
      "backend": backend,
      "database_url": database_url,
      "postgres_url": postgres_url,
      "sqlite_path": os.path.abspath(sqlite_path),
   }


def state_log_runtime_config() -> dict:
   stored = get_setting(STATE_LOG_DB_CONFIG_KEY, {})
   return _normalize_state_log_config(stored)


def state_log_default_config() -> dict:
   return _state_log_defaults()


def set_state_log_runtime_config(config: dict) -> dict:
   normalized = _normalize_state_log_config(config)
   set_setting(STATE_LOG_DB_CONFIG_KEY, normalized)
   get_state_log_session_factory.cache_clear()
   get_state_log_engine.cache_clear()
   return normalized


def state_log_backend() -> str:
   return state_log_runtime_config()["backend"]


def state_log_database_url() -> str:
   current = state_log_runtime_config()
   explicit = str(current.get("database_url", "")).strip()
   if explicit != "":
      return explicit

   if str(current.get("backend")) == "sqlite":
      sqlite_path = str(current.get("sqlite_path", DEFAULT_SQLITE_PATH)).strip()
      if sqlite_path == "":
         sqlite_path = DEFAULT_SQLITE_PATH
      return "sqlite:///" + os.path.abspath(sqlite_path)

   postgres_url = str(current.get("postgres_url", DEFAULT_POSTGRES_URL)).strip()
   if postgres_url == "":
      postgres_url = DEFAULT_POSTGRES_URL
   return postgres_url


def _engine_kwargs() -> dict:
   database_url = state_log_database_url()
   kwargs = {
      "future": True,
      "pool_pre_ping": True,
   }
   if database_url.startswith("sqlite:///"):
      kwargs["connect_args"] = {"check_same_thread": False, "timeout": 2.0}
   else:
      kwargs["pool_size"] = int(os.getenv("METIS_STATE_LOG_DB_POOL_SIZE", "5"))
      kwargs["max_overflow"] = int(os.getenv("METIS_STATE_LOG_DB_MAX_OVERFLOW", "10"))
      kwargs["pool_timeout"] = int(os.getenv("METIS_STATE_LOG_DB_POOL_TIMEOUT_SEC", "5"))
   return kwargs


@lru_cache(maxsize=1)
def get_state_log_engine() -> Engine:
   return create_engine(state_log_database_url(), **_engine_kwargs())


@lru_cache(maxsize=1)
def get_state_log_session_factory():
   return sessionmaker(bind=get_state_log_engine(), autoflush=False, autocommit=False, expire_on_commit=False)


def state_log_schema_ready() -> tuple:
   try:
      engine = get_state_log_engine()
      inspector = inspect(engine)
      existing = set(inspector.get_table_names())
      missing = [name for name in EXPECTED_TABLES if name not in existing]
      if missing:
         return False, "missing tables: " + ",".join(missing)

      with engine.connect() as conn:
         version = conn.execute(text("SELECT version_num FROM alembic_version LIMIT 1")).scalar_one_or_none()
      if version is None or str(version).strip() == "":
         return False, "alembic_version is empty"
      return True, ""
   except Exception as ex:
      return False, str(ex)