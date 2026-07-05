import json
import os
import subprocess
import threading
import time
from collections import deque
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from metis_fastapi.dependencies import get_redis_proxy
from modules.local_settings import get_setting, set_setting, get_settings_file_path
from modules.state_log_db import (
   get_state_log_engine,
   set_state_log_runtime_config,
   state_log_backend,
   state_log_database_url,
   state_log_default_config,
   state_log_runtime_config,
   state_log_schema_ready,
)
from modules.state_log_repository import (
   AnomalyPayload,
   SQLAlchemyStateLogRepository,
   StateLogRepositoryError,
   TransitionPayload,
)
router = APIRouter(
   prefix="/nestdaq",
   tags=["nestdaq"]
)


aProxy = get_redis_proxy(0)

_health_lock = threading.Lock()
_event_cv = threading.Condition()
_config_lock = threading.Lock()
_monitor_thread: Optional[threading.Thread] = None
_monitor_interval_sec = float(os.getenv("NESTDAQ_MONITOR_INTERVAL_SEC", "1.0"))
_stale_threshold_sec = float(os.getenv("NESTDAQ_STALE_THRESHOLD_SEC", "5.0"))
_event_buffer_max = int(os.getenv("NESTDAQ_EVENT_BUFFER_MAX", "500"))
_recovery_cooldown_sec = float(os.getenv("NESTDAQ_RECOVERY_COOLDOWN_SEC", "60.0"))
_recovery_timeout_sec = int(os.getenv("NESTDAQ_RECOVERY_TIMEOUT_SEC", "120"))
_auto_recovery_enabled = os.getenv("NESTDAQ_AUTO_RECOVERY", "0") == "1"
_auto_resume_after_recovery_enabled = os.getenv("NESTDAQ_AUTO_RESUME_AFTER_RECOVERY", "1") == "1"
_resume_prepare_timeout_sec = int(os.getenv("NESTDAQ_RESUME_PREPARE_TIMEOUT_SEC", "90"))
_recovery_script = os.getenv(
   "NESTDAQ_RECOVERY_SCRIPT",
   "/home/akeno/repos/workdir-akeno-spadi/scripts/run-nestdaq-akeno.sh",
)
_expected_services = [
   s.strip()
   for s in os.getenv(
      "NESTDAQ_EXPECTED_SERVICES",
      "",
   ).split(",")
   if s.strip() != ""
]
_observed_services: Set[str] = set()
_monitor_config_defaults = {
   "monitor_interval_sec": float(os.getenv("NESTDAQ_MONITOR_INTERVAL_SEC", "1.0")),
   "stale_threshold_sec": float(os.getenv("NESTDAQ_STALE_THRESHOLD_SEC", "5.0")),
   "recovery_cooldown_sec": float(os.getenv("NESTDAQ_RECOVERY_COOLDOWN_SEC", "60.0")),
   "recovery_timeout_sec": int(os.getenv("NESTDAQ_RECOVERY_TIMEOUT_SEC", "120")),
   "auto_recovery_enabled": os.getenv("NESTDAQ_AUTO_RECOVERY", "0") == "1",
   "auto_resume_after_recovery_enabled": os.getenv("NESTDAQ_AUTO_RESUME_AFTER_RECOVERY", "1") == "1",
   "resume_prepare_timeout_sec": int(os.getenv("NESTDAQ_RESUME_PREPARE_TIMEOUT_SEC", "90")),
   "recovery_script": os.getenv(
      "NESTDAQ_RECOVERY_SCRIPT",
      "/home/akeno/repos/workdir-akeno-spadi/scripts/run-nestdaq-akeno.sh",
   ),
   "expected_services": [
      s.strip()
      for s in os.getenv("NESTDAQ_EXPECTED_SERVICES", "").split(",")
      if s.strip() != ""
   ],
}

_events = deque(maxlen=_event_buffer_max)
_state_log_target = state_log_database_url()
_state_log_queue_max = int(os.getenv("NESTDAQ_STATE_LOG_QUEUE_MAX", "20000"))
_state_log_retry_sec = float(os.getenv("NESTDAQ_STATE_LOG_RETRY_SEC", "2.0"))
_state_log_lock = threading.Lock()
_state_log_cv = threading.Condition(_state_log_lock)
_state_log_queue = deque(maxlen=_state_log_queue_max)
_state_log_writer_thread: Optional[threading.Thread] = None
_state_log_repository = SQLAlchemyStateLogRepository()
_state_log_status = {
   "enabled": True,
   "backend": state_log_backend(),
   "db_target": _state_log_target,
   "db_available": False,
   "pending_records": 0,
   "total_written": 0,
   "total_dropped": 0,
   "last_write_ts": 0,
   "last_error": "",
   "last_error_ts": 0,
}
_state_log_db_notified_unavailable = False
_last_summary = {
   "overall": "unknown",
   "service_count": 0,
   "missing_expected_services": [],
   "process_health": "unknown",
   "mismatch": False,
   "stale_services": [],
   "state": "",
   "states": {},
   "updated": {},
   "last_checked_ts": 0,
   "auto_recovery_enabled": _auto_recovery_enabled,
   "auto_resume_after_recovery_enabled": _auto_resume_after_recovery_enabled,
}
_last_recovery_ts = 0.0
_run_was_active_since_idle = False
_transition_watch: Optional[Dict[str, Any]] = None
_auto_run_change_lock = threading.Lock()
_auto_run_change_thread: Optional[threading.Thread] = None
_state_change_timeout_sec = int(os.getenv("NESTDAQ_STATE_CHANGE_TIMEOUT_SEC", "20"))
_state_change_wait_interval_sec = float(os.getenv("NESTDAQ_STATE_CHANGE_WAIT_INTERVAL_SEC", "1.0"))

_daq_state_destination = {
   "CONNECT": "DEVICE READY",
   "INIT TASK": "READY",
   "RUN": "RUNNING",
   "STOP": "READY",
   "RESET TASK": "DEVICE READY",
   "RESET DEVICE": "IDLE",
   "END": "NO PROCESS",
}

_hook_cmd_key = {
   "PRE START": "run_info:pre_start_script",
   "POST START": "run_info:post_start_script",
   "PRE STOP": "run_info:pre_stop_script",
   "POST STOP": "run_info:post_stop_script",
}

_auto_run_change_sequence = [
   "PRE STOP",
   "STOP",
   "RESET TASK",
   "RESET DEVICE",
   "POST STOP",
   "PRE START",
   "CONNECT",
   "INIT TASK",
   "RUN",
   "POST START",
]


def _normalize_expected_services(value) -> List[str]:
   if isinstance(value, str):
      seq = value.split(",")
   elif isinstance(value, list):
      seq = value
   else:
      seq = []
   return sorted({str(v).strip() for v in seq if str(v).strip() != ""})


def _monitor_config_dict() -> Dict:
   return {
      "monitor_interval_sec": _monitor_interval_sec,
      "stale_threshold_sec": _stale_threshold_sec,
      "recovery_cooldown_sec": _recovery_cooldown_sec,
      "recovery_timeout_sec": _recovery_timeout_sec,
      "auto_recovery_enabled": _auto_recovery_enabled,
      "auto_resume_after_recovery_enabled": _auto_resume_after_recovery_enabled,
      "resume_prepare_timeout_sec": _resume_prepare_timeout_sec,
      "recovery_script": _recovery_script,
      "expected_services": list(_expected_services),
   }


def _apply_monitor_config(config: Dict):
   global _monitor_interval_sec
   global _stale_threshold_sec
   global _recovery_cooldown_sec
   global _recovery_timeout_sec
   global _auto_recovery_enabled
   global _auto_resume_after_recovery_enabled
   global _resume_prepare_timeout_sec
   global _recovery_script
   global _expected_services

   _monitor_interval_sec = max(0.2, float(config.get("monitor_interval_sec", _monitor_config_defaults["monitor_interval_sec"])))
   _stale_threshold_sec = max(0.5, float(config.get("stale_threshold_sec", _monitor_config_defaults["stale_threshold_sec"])))
   _recovery_cooldown_sec = max(1.0, float(config.get("recovery_cooldown_sec", _monitor_config_defaults["recovery_cooldown_sec"])))
   _recovery_timeout_sec = max(10, int(config.get("recovery_timeout_sec", _monitor_config_defaults["recovery_timeout_sec"])))
   _auto_recovery_enabled = bool(config.get("auto_recovery_enabled", _monitor_config_defaults["auto_recovery_enabled"]))
   _auto_resume_after_recovery_enabled = bool(
      config.get("auto_resume_after_recovery_enabled", _monitor_config_defaults["auto_resume_after_recovery_enabled"])
   )
   _resume_prepare_timeout_sec = max(
      10,
      int(config.get("resume_prepare_timeout_sec", _monitor_config_defaults["resume_prepare_timeout_sec"])),
   )
   _recovery_script = str(config.get("recovery_script", _monitor_config_defaults["recovery_script"]))
   _expected_services = _normalize_expected_services(
      config.get("expected_services", _monitor_config_defaults["expected_services"])
   )


def _load_monitor_config_from_settings():
   stored = get_setting("nestdaq_monitor_config", {})
   if not isinstance(stored, dict):
      stored = {}
   merged = dict(_monitor_config_defaults)
   merged.update(stored)
   _apply_monitor_config(merged)


def _save_monitor_config_to_settings(config: Dict):
   set_setting("nestdaq_monitor_config", config)


def _rcli():
   rcli = aProxy.instance()
   if rcli is None:
      raise HTTPException(status_code=503, detail="Redis client is not connected")
   return rcli


def _to_text(v):
   if isinstance(v, bytes):
      return v.decode(errors="replace")
   return v


def _now_ts() -> int:
   # millisecond epoch to avoid collisions when multiple events happen in 1 second.
   return int(time.time() * 1000)


def _emit_event(
   event_type: str,
   severity: str,
   source: str,
   state_before: str,
   state_after: str,
   action: str,
   result: str,
):
   event = {
      "event_type": event_type,
      "severity": severity,
      "source": source,
      "state_before": state_before,
      "state_after": state_after,
      "action": action,
      "result": result,
      "ts": _now_ts(),
   }
   with _event_cv:
      _events.append(event)
      _event_cv.notify_all()


def _current_run_number(rcli) -> Optional[int]:
   if rcli is None:
      return None
   text = _read_text_key(rcli, "run_info:run_number", "").strip()
   if text == "":
      return None
   try:
      return int(float(text))
   except Exception:
      return None


def _latest_run_number(rcli) -> Optional[int]:
   if rcli is None:
      return None
   text = _read_text_key(rcli, "run_info:latest_run_number", "").strip()
   if text == "":
      return None
   try:
      return int(float(text))
   except Exception:
      return None


def _monitoring_run_number(rcli, before_state: str, current_state: str) -> Optional[int]:
   b = str(before_state or "")
   c = str(current_state or "")
   if b == "RUNNING" or c == "RUNNING":
      latest = _latest_run_number(rcli)
      if latest is not None:
         return latest
   return _current_run_number(rcli)


def _begin_transition_watch(
   mode: str,
   run_number: Optional[int],
   current_state: str,
   trigger: str = "monitor-fallback",
   command: str = "",
):
   global _transition_watch
   _transition_watch = {
      "mode": str(mode),
      "run_number": run_number,
      "last_state": str(current_state),
      "last_change_ts": _now_ts(),
      "started_ts": _now_ts(),
      "trigger": str(trigger),
      "command": str(command),
   }


def _arm_transition_watch_from_command(rcli, command: str):
   cmd = str(command or "").strip().upper()
   if cmd == "":
      return

   current_state = _merged_daq_state(rcli)
   if cmd == "RUN":
      _begin_transition_watch(
         mode="start",
         run_number=_current_run_number(rcli),
         current_state=current_state,
         trigger="command",
         command=cmd,
      )
      return

   if cmd in {"STOP", "RESET DEVICE", "END"}:
      _begin_transition_watch(
         mode="end",
         run_number=_latest_run_number(rcli),
         current_state=current_state,
         trigger="command",
         command=cmd,
      )


def _record_transition_watch_result(mode: str, run_number: Optional[int], final_state: str, timed_out: bool, detail: Dict[str, Any]):
   now_ts = _now_ts()
   def _collect_transition_telemetry_snapshot() -> Dict[str, Any]:
      snapshot: Dict[str, Any] = {
         "captured_ts_ms": _now_ts(),
         "scaler": {},
         "hv": {},
      }
      try:
         from routers import RouterScaler
         snapshot["scaler"] = RouterScaler.factory.get_data_cached()
      except Exception as ex:
         snapshot["scaler"] = {"error": str(ex)}
      try:
         from routers import RouterRph032
         snapshot["hv"] = RouterRph032.export_cached_all()
      except Exception as ex:
         snapshot["hv"] = {"error": str(ex)}
      return snapshot

   if mode == "start":
      if timed_out:
         _record_anomaly(
            event_ts_ms=now_ts,
            anomaly_type="run_start_timeout",
            severity="warning",
            state=str(final_state),
            run_number=run_number,
            message=f"Run start sequence timed out before RUNNING (last_state={final_state})",
            detail=detail,
         )
      else:
         transition_detail = dict(detail)
         transition_detail["telemetry_snapshot"] = _collect_transition_telemetry_snapshot()
         _record_state_transition(
            event_ts_ms=now_ts,
            transition_type="run_start",
            from_state="IDLE",
            to_state="RUNNING",
            run_number=run_number,
            message="Detected IDLE => RUNNING transition sequence",
            detail=transition_detail,
         )
      return

   if mode == "end":
      if timed_out:
         _record_anomaly(
            event_ts_ms=now_ts,
            anomaly_type="run_end_timeout",
            severity="warning",
            state=str(final_state),
            run_number=run_number,
            message=f"Run end sequence timed out before IDLE (last_state={final_state})",
            detail=detail,
         )
      else:
         transition_detail = dict(detail)
         transition_detail["telemetry_snapshot"] = _collect_transition_telemetry_snapshot()
         _record_state_transition(
            event_ts_ms=now_ts,
            transition_type="run_end",
            from_state="RUNNING",
            to_state="IDLE",
            run_number=run_number,
            message="Detected RUNNING => IDLE transition sequence",
            detail=transition_detail,
         )


def _update_transition_watch(current_state: str):
   global _transition_watch

   watch = _transition_watch
   if watch is None:
      return

   now_ts = _now_ts()
   prev_state = str(watch.get("last_state", ""))
   state_now = str(current_state)
   if state_now != prev_state:
      watch["last_state"] = state_now
      watch["last_change_ts"] = now_ts

   mode = str(watch.get("mode", ""))
   run_number = watch.get("run_number", None)
   trigger = str(watch.get("trigger", ""))
   command = str(watch.get("command", ""))
   started_ts = int(watch.get("started_ts", now_ts))
   last_change_ts = int(watch.get("last_change_ts", now_ts))
   timeout_ms = int(max(1, _state_change_timeout_sec) * 1000)

   if mode == "start" and state_now == "RUNNING":
      _record_transition_watch_result(
         mode="start",
         run_number=run_number,
         final_state=state_now,
         timed_out=False,
         detail={
            "sequence": "idle_to_running",
            "timeout_sec": _state_change_timeout_sec,
            "elapsed_ms": now_ts - started_ts,
            "trigger": trigger,
            "command": command,
         },
      )
      _transition_watch = None
      return

   if mode == "end" and state_now == "IDLE":
      _record_transition_watch_result(
         mode="end",
         run_number=run_number,
         final_state=state_now,
         timed_out=False,
         detail={
            "sequence": "running_to_idle",
            "timeout_sec": _state_change_timeout_sec,
            "elapsed_ms": now_ts - started_ts,
            "trigger": trigger,
            "command": command,
         },
      )
      _transition_watch = None
      return

   if now_ts - started_ts > timeout_ms:
      _record_transition_watch_result(
         mode=mode,
         run_number=run_number,
         final_state=state_now,
         timed_out=True,
         detail={
            "sequence": "idle_to_running" if mode == "start" else "running_to_idle",
            "timeout_sec": _state_change_timeout_sec,
            "elapsed_ms": now_ts - started_ts,
            "last_change_elapsed_ms": now_ts - last_change_ts,
            "trigger": trigger,
            "command": command,
         },
      )
      _transition_watch = None


def _state_log_enqueue(record: Dict[str, Any]):
   with _state_log_cv:
      if len(_state_log_queue) >= _state_log_queue.maxlen:
         _state_log_queue.popleft()
         _state_log_status["total_dropped"] = int(_state_log_status.get("total_dropped", 0)) + 1
      _state_log_queue.append(record)
      _state_log_status["pending_records"] = len(_state_log_queue)
      _state_log_cv.notify_all()


def _state_log_write_one(record: Dict[str, Any]):
   record_kind = str(record.get("record_kind", ""))
   event_ts_ms = int(record.get("event_ts_ms", _now_ts()))

   if record_kind == "transition":
      _state_log_repository.record_transition(
         TransitionPayload(
            event_ts_ms=event_ts_ms,
            transition_type=str(record.get("transition_type", "state_transition")),
            from_state=str(record.get("from_state", "")),
            to_state=str(record.get("to_state", "")),
            run_number=record.get("run_number", None),
            source=str(record.get("source", "nestdaq-monitor")),
            message=str(record.get("message", "")),
            detail=dict(record.get("detail", {})),
         )
      )
      return

   if record_kind == "anomaly":
      _state_log_repository.record_anomaly(
         AnomalyPayload(
            event_ts_ms=event_ts_ms,
            anomaly_type=str(record.get("anomaly_type", "anomaly")),
            severity=str(record.get("severity", "critical")),
            state=str(record.get("state", "")),
            run_number=record.get("run_number", None),
            source=str(record.get("source", "nestdaq-monitor")),
            message=str(record.get("message", "")),
            detail=dict(record.get("detail", {})),
         )
      )


def _state_log_set_error(err: str):
   global _state_log_db_notified_unavailable
   should_emit = False
   with _state_log_lock:
      _state_log_status["db_available"] = False
      _state_log_status["last_error"] = err
      _state_log_status["last_error_ts"] = _now_ts()
      _state_log_status["pending_records"] = len(_state_log_queue)
      if not _state_log_db_notified_unavailable:
         _state_log_db_notified_unavailable = True
         should_emit = True
   if should_emit:
      _emit_event(
         event_type="state_log_db_unavailable",
         severity="warning",
         source="state-log-db",
         state_before="",
         state_after="",
         action="write",
         result=err[:200],
      )


def _state_log_set_recovered():
   global _state_log_db_notified_unavailable
   should_emit = False
   with _state_log_lock:
      _state_log_status["db_available"] = True
      _state_log_status["last_error"] = ""
      _state_log_status["pending_records"] = len(_state_log_queue)
      if _state_log_db_notified_unavailable:
         _state_log_db_notified_unavailable = False
         should_emit = True
   if should_emit:
      _emit_event(
         event_type="state_log_db_recovered",
         severity="info",
         source="state-log-db",
         state_before="",
         state_after="",
         action="write",
         result="success",
      )


def _state_log_writer_worker():
   schema_ready = False
   while True:
      with _state_log_cv:
         if len(_state_log_queue) == 0:
            _state_log_cv.wait(timeout=1.0)
            continue
         record = dict(_state_log_queue[0])

      try:
         if not schema_ready:
            _state_log_repository.check_schema_ready()
            schema_ready = True

         _state_log_write_one(record)

         with _state_log_lock:
            if len(_state_log_queue) > 0:
               _state_log_queue.popleft()
            _state_log_status["pending_records"] = len(_state_log_queue)
            _state_log_status["total_written"] = int(_state_log_status.get("total_written", 0)) + 1
            _state_log_status["last_write_ts"] = _now_ts()
         _state_log_set_recovered()
      except Exception as ex:
         schema_ready = False
         _state_log_set_error(str(ex))
         wait_sec = _state_log_retry_sec if _state_log_retry_sec > 0 else 1.0
         time.sleep(wait_sec)


def _start_state_log_writer_if_needed():
   global _state_log_writer_thread
   if _state_log_writer_thread is None or not _state_log_writer_thread.is_alive():
      _state_log_writer_thread = threading.Thread(target=_state_log_writer_worker, daemon=True)
      _state_log_writer_thread.start()


def _record_state_transition(
   event_ts_ms: int,
   transition_type: str,
   from_state: str,
   to_state: str,
   run_number: Optional[int],
   message: str,
   detail: Dict[str, Any],
):
   _state_log_enqueue(
      {
         "record_kind": "transition",
         "event_ts_ms": event_ts_ms,
         "transition_type": transition_type,
         "from_state": str(from_state),
         "to_state": str(to_state),
         "run_number": run_number,
         "source": "nestdaq-monitor",
         "message": message,
         "detail": detail,
      }
   )


def _record_anomaly(
   event_ts_ms: int,
   anomaly_type: str,
   severity: str,
   state: str,
   run_number: Optional[int],
   message: str,
   detail: Dict[str, Any],
):
   _state_log_enqueue(
      {
         "record_kind": "anomaly",
         "event_ts_ms": event_ts_ms,
         "anomaly_type": anomaly_type,
         "severity": severity,
         "state": str(state),
         "run_number": run_number,
         "source": "nestdaq-monitor",
         "message": message,
         "detail": detail,
      }
   )


def _decode_service_name(key: bytes) -> str:
   text = _to_text(key)
   # Preserve legacy behavior used by /status/ which picked index 2.
   parts = str(text).split(":")
   if len(parts) >= 3:
      return parts[2]
   return str(text)


def _parse_unix_sec(value: Optional[str]) -> Optional[float]:
   if value is None:
      return None
   text = str(value).strip()
   if text == "":
      return None
   try:
      return float(text)
   except Exception:
      return None


def _collect_status(rcli) -> Dict:
   key_updated = rcli.keys("daq_service:*:updatedTime")
   key_state = rcli.keys("daq_service:*:fair-mq-state")

   state_map = {}
   updated_map = {}

   if key_state:
      val_state = rcli.mget(key_state)
      for k, v in zip(key_state, val_state):
         state_map[_decode_service_name(k)] = _to_text(v) or ""

   if key_updated:
      val_updated = rcli.mget(key_updated)
      for k, v in zip(key_updated, val_updated):
         updated_map[_decode_service_name(k)] = _to_text(v)

   return {
      "states": state_map,
      "updated": updated_map,
   }


def _evaluate_summary(status: Dict) -> Dict:
   global _observed_services

   states = status.get("states", {})
   updated = status.get("updated", {})
   now = time.time()
   unique_states = sorted({str(v) for v in states.values() if str(v) != ""})

   if len(states) == 0:
      overall = "critical"
      merged_state = "NO PROCESS"
      mismatch = False
   else:
      mismatch = len(unique_states) > 1
      merged_state = unique_states[0] if len(unique_states) == 1 else "MISMATCH"
      overall = "warning" if mismatch else "ok"

   stale_services: List[str] = []
   for service, ts_text in updated.items():
      ts = _parse_unix_sec(ts_text)
      if ts is None:
         continue
      if now - ts > _stale_threshold_sec:
         stale_services.append(service)

   if stale_services:
      overall = "critical" if len(states) > 0 else overall

   live_services = {str(name) for name in states.keys() if str(name) != ""}
   if len(live_services) > 0:
      _observed_services = _observed_services.union(live_services)
   if len(_expected_services) > 0:
      expected_services = set(_expected_services)
   elif len(_observed_services) > 0:
      expected_services = set(_observed_services)
   else:
      expected_services = set(live_services)
   missing_expected_services = sorted([s for s in expected_services if s not in live_services])
   process_health = "ok" if len(missing_expected_services) == 0 else "critical"
   if len(missing_expected_services) > 0:
      overall = "critical"

   return {
      "overall": overall,
      "service_count": len(states),
      "missing_expected_services": missing_expected_services,
      "process_health": process_health,
      "mismatch": mismatch,
      "stale_services": sorted(stale_services),
      "state": merged_state,
      "states": states,
      "updated": updated,
      "last_checked_ts": _now_ts(),
      "auto_recovery_enabled": _auto_recovery_enabled,
      "auto_resume_after_recovery_enabled": _auto_resume_after_recovery_enabled,
   }


def _run_recovery_action(action: str) -> Dict:
   if action != "restart_all":
      raise HTTPException(status_code=400, detail="unsupported action")

   redis_host = os.getenv("REDIS_SERVER_HOST", "localhost")
   redis_port = os.getenv("REDIS_SERVER_PORT", "6379")
   cmd = ["/bin/sh", _recovery_script, redis_host, str(redis_port)]
   completed = subprocess.run(
      cmd,
      capture_output=True,
      text=True,
      timeout=_recovery_timeout_sec,
      check=False,
   )
   return {
      "action": action,
      "returncode": completed.returncode,
      "stdout": completed.stdout[-4000:],
      "stderr": completed.stderr[-4000:],
   }


def _read_text_key(rcli, key: str, default: str = "") -> str:
   try:
      raw = rcli.get(key)
   except Exception:
      return default
   text = _to_text(raw)
   if text is None:
      return default
   return str(text)


def _read_int_key(rcli, key: str, default: int = 0) -> int:
   text = _read_text_key(rcli, key, "").strip()
   if text == "":
      return default
   try:
      return int(float(text))
   except Exception:
      return default


def _merged_daq_state(rcli) -> str:
   status = _collect_status(rcli)
   states = status.get("states", {})
   if len(states) == 0:
      return "NO PROCESS"
   unique_states = sorted({str(v) for v in states.values() if str(v) != ""})
   if len(unique_states) == 1:
      return unique_states[0]
   return "MISMATCH"


def _publish_change_state(rcli, command: str):
   payload = json.dumps(
      {
         "command": "change_state",
         "value": command,
         "services": ["all"],
         "instances": ["all"],
      },
      separators=(",", ":"),
      ensure_ascii=True,
   )
   rcli.publish("daqctl", payload)
   _arm_transition_watch_from_command(rcli, command)


def _wait_for_state_change(rcli, destination: str, timeout_sec: int) -> bool:
   start = time.time()
   while time.time() - start < timeout_sec:
      if _merged_daq_state(rcli) == destination:
         return True
      wait_sec = _state_change_wait_interval_sec if _state_change_wait_interval_sec > 0 else 1.0
      time.sleep(wait_sec)
   return False


def _run_hook_script(rcli, stage: str, hook_key: str):
   cmd = _read_text_key(rcli, hook_key, "").strip()
   if cmd == "":
      return

   try:
      completed = subprocess.run(
         cmd,
         shell=True,
         capture_output=True,
         text=True,
         timeout=_recovery_timeout_sec,
         check=False,
      )
      if completed.returncode != 0:
         rcli.set("run_info:daq_error_status", "script_error:" + hook_key)
         _emit_event(
            event_type="hook_script_failed",
            severity="warning",
            source="auto-run-change",
            state_before=stage,
            state_after=stage,
            action=cmd,
            result="failed",
         )
   except Exception:
      rcli.set("run_info:daq_error_status", "script_error:" + hook_key)
      _emit_event(
         event_type="hook_script_failed",
         severity="warning",
         source="auto-run-change",
         state_before=stage,
         state_after=stage,
         action=cmd,
         result="failed",
      )


def _run_auto_run_change_sequence_worker():
   rcli = aProxy.instance()
   if rcli is None:
      return

   rcli.set("run_info:daq_error_status", "")
   _emit_event(
      event_type="auto_run_change_started",
      severity="info",
      source="auto-run-change",
      state_before="RUNNING",
      state_after="RUNNING",
      action="rotate_run",
      result="running",
   )

   for cmd in _auto_run_change_sequence:
      if cmd in _hook_cmd_key:
         _run_hook_script(rcli, cmd, _hook_cmd_key[cmd])
         continue

      if cmd == "STOP":
         rcli.incr("run_info:run_number")

      if cmd == "RUN":
         run_ts = int(time.time())
         rcli.set("run_info:run_start_unix_time", str(run_ts))
         run_number = _read_text_key(rcli, "run_info:run_number", "")
         rcli.set("run_info:latest_run_number", run_number)

      try:
         _publish_change_state(rcli, cmd)
      except Exception:
         rcli.set("run_info:daq_error_status", "publish_error")
         _emit_event(
            event_type="auto_run_change_failed",
            severity="critical",
            source="auto-run-change",
            state_before=cmd,
            state_after=cmd,
            action="publish",
            result="failed",
         )
         return

      destination = _daq_state_destination.get(cmd)
      if destination and (not _wait_for_state_change(rcli, destination, _state_change_timeout_sec)):
         rcli.set("run_info:daq_error_status", "wait_for_state_change")
         _emit_event(
            event_type="auto_run_change_failed",
            severity="critical",
            source="auto-run-change",
            state_before=cmd,
            state_after=destination,
            action="wait_state",
            result="timeout",
         )
         return

   _emit_event(
      event_type="auto_run_change_finished",
      severity="info",
      source="auto-run-change",
      state_before="RUNNING",
      state_after="RUNNING",
      action="rotate_run",
      result="success",
   )


def _start_auto_run_change_if_needed(summary: Dict, rcli):
   global _auto_run_change_thread

   if summary.get("state") != "RUNNING":
      return

   mode = _read_text_key(rcli, "run_info:auto_run_change_mode", "")
   if mode != "enabled":
      return

   duration_sec = _read_int_key(rcli, "run_info:auto_run_change_dur", 0)
   if duration_sec <= 0:
      return

   start_unix_time = _read_int_key(rcli, "run_info:run_start_unix_time", 0)
   if start_unix_time <= 0:
      return

   elapsed_sec = int(time.time()) - start_unix_time
   if elapsed_sec < duration_sec:
      return

   with _auto_run_change_lock:
      if _auto_run_change_thread is not None and _auto_run_change_thread.is_alive():
         return
      # Set to 0 before starting so another worker does not retrigger immediately.
      rcli.set("run_info:run_start_unix_time", "0")
      _auto_run_change_thread = threading.Thread(
         target=_run_auto_run_change_sequence_worker,
         daemon=True,
      )
      _auto_run_change_thread.start()


def _run_data_collection_resume_sequence(rcli, sequence: List[str]) -> bool:
   for cmd in sequence:
      if cmd in _hook_cmd_key:
         _run_hook_script(rcli, cmd, _hook_cmd_key[cmd])
         continue

      if cmd == "RUN":
         run_ts = int(time.time())
         rcli.set("run_info:run_start_unix_time", str(run_ts))
         run_number = _read_text_key(rcli, "run_info:run_number", "")
         rcli.set("run_info:latest_run_number", run_number)

      try:
         _publish_change_state(rcli, cmd)
      except Exception:
         rcli.set("run_info:daq_error_status", "publish_error")
         return False

      destination = _daq_state_destination.get(cmd)
      if destination and (not _wait_for_state_change(rcli, destination, _state_change_timeout_sec)):
         rcli.set("run_info:daq_error_status", "wait_for_state_change")
         return False
   return True


def _resume_data_collection_after_recovery(rcli, before_state: str):
   if (not _auto_resume_after_recovery_enabled) or before_state != "RUNNING":
      return

   start = time.time()
   current_state = _merged_daq_state(rcli)
   while current_state not in {"IDLE", "DEVICE READY", "READY", "RUNNING"}:
      if time.time() - start >= _resume_prepare_timeout_sec:
         _emit_event(
            event_type="resume_after_recovery_failed",
            severity="critical",
            source="nestdaq-monitor",
            state_before=before_state,
            state_after=current_state,
            action="wait_ready_state",
            result="timeout",
         )
         return
      time.sleep(1.0)
      current_state = _merged_daq_state(rcli)

   if current_state == "RUNNING":
      _emit_event(
         event_type="resume_after_recovery_finished",
         severity="info",
         source="nestdaq-monitor",
         state_before=before_state,
         state_after=current_state,
         action="resume_run",
         result="success",
      )
      return

   if current_state == "IDLE":
      sequence = ["PRE START", "CONNECT", "INIT TASK", "RUN", "POST START"]
   elif current_state == "DEVICE READY":
      sequence = ["INIT TASK", "RUN", "POST START"]
   else:
      sequence = ["RUN", "POST START"]

   ok = _run_data_collection_resume_sequence(rcli, sequence)
   _emit_event(
      event_type="resume_after_recovery_finished" if ok else "resume_after_recovery_failed",
      severity="info" if ok else "critical",
      source="nestdaq-monitor",
      state_before=before_state,
      state_after=_merged_daq_state(rcli),
      action="resume_run",
      result="success" if ok else "failed",
   )


def _should_increment_run_number_on_recovery(before_state: str, run_was_active_since_idle: bool) -> bool:
   b = str(before_state or "")
   if b == "RUNNING":
      return True
   if run_was_active_since_idle and b in {"READY", "DEVICE READY"}:
      return True
   return False


def _monitor_once():
   global _last_summary
   global _last_recovery_ts
   global _run_was_active_since_idle
   global _transition_watch

   before = _last_summary
   rcli = aProxy.instance()
   if rcli is None:
      summary = {
         "overall": "critical",
         "service_count": 0,
         "missing_expected_services": [],
         "process_health": "critical",
         "mismatch": False,
         "stale_services": [],
         "state": "NO REDIS",
         "states": {},
         "updated": {},
         "last_checked_ts": _now_ts(),
         "auto_recovery_enabled": _auto_recovery_enabled,
         "auto_resume_after_recovery_enabled": _auto_resume_after_recovery_enabled,
      }
   else:
      summary = _evaluate_summary(_collect_status(rcli))

   before_state = str(before.get("state", ""))
   current_state = str(summary.get("state", ""))
   run_number = _current_run_number(rcli)
   monitoring_run_number = _monitoring_run_number(rcli, before_state, current_state)

   # First, try to resolve an existing watch (success/timeout).
   _update_transition_watch(current_state)

   watch = _transition_watch
   if watch is not None:
      watch_mode = str(watch.get("mode", ""))
      if watch_mode == "end" and current_state == "RUNNING":
         # Recovery came back to RUNNING before reaching IDLE.
         # Close stale end-watch path and record a new run_start immediately.
         _transition_watch = None
         _begin_transition_watch(
            mode="start",
            run_number=run_number,
            current_state=current_state,
         )
         _update_transition_watch(current_state)

   if _transition_watch is None:
      if before_state == "IDLE" and current_state != "IDLE":
         _begin_transition_watch(
            mode="start",
            run_number=run_number,
            current_state=current_state,
            trigger="monitor-fallback",
            command="",
         )
         _update_transition_watch(current_state)
      elif before_state == "RUNNING" and current_state != "RUNNING":
         _begin_transition_watch(
            mode="end",
            run_number=_latest_run_number(rcli),
            current_state=current_state,
            trigger="monitor-fallback",
            command="",
         )
         _update_transition_watch(current_state)
      elif before_state not in {"", "RUNNING"} and current_state == "RUNNING":
         # Fallback path for fast recovery transitions where IDLE phase is skipped in polling.
         _begin_transition_watch(
            mode="start",
            run_number=run_number,
            current_state=current_state,
            trigger="monitor-fallback",
            command="",
         )
         _update_transition_watch(current_state)

   before_missing = before.get("missing_expected_services", [])
   before_stale = before.get("stale_services", [])
   before_overall = str(before.get("overall", ""))
   restart_recovery_context = (
      before_state not in {"", "IDLE", "RUNNING"}
      or before_overall == "critical"
      or len(before_missing) > 0
      or len(before_stale) > 0
   )
   recovered_now = (
      before_overall == "critical"
      and str(summary.get("overall", "")) != "critical"
   )
   state_transitioned = before_state != current_state
   if restart_recovery_context and current_state in {"IDLE", "RUNNING"} and (state_transitioned or recovered_now):
      _record_anomaly(
         event_ts_ms=_now_ts(),
         anomaly_type="restart_from_intermediate_state",
         severity="info",
         state=current_state,
         run_number=monitoring_run_number,
         message=f"DAQ restart/recovery observed from intermediate state ({before_state} => {current_state})",
         detail={
            "before_state": before_state,
            "after_state": current_state,
            "before_overall": before_overall,
            "missing_expected_services": before_missing,
            "stale_services": before_stale,
         },
      )

   if before.get("overall") != "critical" and summary.get("overall") == "critical":
      _record_anomaly(
         event_ts_ms=_now_ts(),
         anomaly_type="health_critical",
         severity="critical",
         state=current_state,
         run_number=monitoring_run_number,
         message="Overall health became critical",
         detail={
            "missing_expected_services": summary.get("missing_expected_services", []),
            "stale_services": summary.get("stale_services", []),
         },
      )

   if current_state == "RUNNING":
      _run_was_active_since_idle = True
   elif current_state == "IDLE":
      _run_was_active_since_idle = False

   if rcli is not None:
      _start_auto_run_change_if_needed(summary, rcli)

   with _health_lock:
      _last_summary = summary

   if before.get("overall") != summary.get("overall"):
      sev = "info"
      if summary["overall"] == "warning":
         sev = "warning"
      if summary["overall"] == "critical":
         sev = "critical"
      _emit_event(
         event_type="health_changed",
         severity=sev,
         source="nestdaq-monitor",
         state_before=str(before.get("state", "")),
         state_after=str(summary.get("state", "")),
         action="none",
         result="success",
      )

   if (not before.get("mismatch", False)) and summary.get("mismatch", False):
      _emit_event(
         event_type="state_mismatch_detected",
         severity="warning",
         source="nestdaq-monitor",
         state_before=str(before.get("state", "")),
         state_after=str(summary.get("state", "")),
         action="none",
         result="success",
      )

   if len(summary.get("stale_services", [])) > 0 and len(before.get("stale_services", [])) == 0:
      _record_anomaly(
         event_ts_ms=_now_ts(),
         anomaly_type="service_stale",
         severity="critical",
         state=current_state,
         run_number=monitoring_run_number,
         message="Detected stale services",
         detail={"stale_services": summary.get("stale_services", [])},
      )
      _emit_event(
         event_type="service_stale_detected",
         severity="critical",
         source="nestdaq-monitor",
         state_before=str(before.get("state", "")),
         state_after=str(summary.get("state", "")),
         action="none",
         result="success",
      )

   # Expected services missing from redis status indicates process-level anomaly.
   if len(summary.get("missing_expected_services", [])) > 0 and len(before.get("missing_expected_services", [])) == 0:
      missing_services = summary.get("missing_expected_services", [])
      _record_anomaly(
         event_ts_ms=_now_ts(),
         anomaly_type="process_missing",
         severity="critical",
         state=current_state,
         run_number=monitoring_run_number,
         message="Detected missing expected services: " + ",".join([str(s) for s in missing_services]),
         detail={"missing_expected_services": missing_services},
      )
      _emit_event(
         event_type="process_missing_detected",
         severity="critical",
         source="nestdaq-monitor",
         state_before=str(before.get("state", "")),
         state_after=str(summary.get("state", "")),
         action="none",
         result=",".join([str(s) for s in missing_services]),
      )

   if _auto_recovery_enabled and summary.get("overall") == "critical":
      now = time.time()
      if now - _last_recovery_ts >= _recovery_cooldown_sec:
         _last_recovery_ts = now
         before_state = str(before.get("state", ""))
         increment_run_number = _should_increment_run_number_on_recovery(before_state, _run_was_active_since_idle)
         _emit_event(
            event_type="recovery_started",
            severity="warning",
            source="nestdaq-monitor",
            state_before=str(summary.get("state", "")),
            state_after=str(summary.get("state", "")),
            action="restart_all",
            result="running",
         )
         try:
            rec = _run_recovery_action("restart_all")
            ok = rec.get("returncode", 1) == 0
            if ok and rcli is not None and increment_run_number:
               new_run_number = rcli.incr("run_info:run_number")
               _emit_event(
                  event_type="run_number_incremented_on_recovery",
                  severity="info",
                  source="nestdaq-monitor",
                  state_before=before_state,
                  state_after=str(summary.get("state", "")),
                  action="increment_run_number",
                  result=str(new_run_number),
               )
            if ok and rcli is not None:
               _resume_data_collection_after_recovery(rcli, before_state)
            _emit_event(
               event_type="recovery_finished",
               severity="info" if ok else "critical",
               source="nestdaq-monitor",
               state_before=str(summary.get("state", "")),
               state_after=str(summary.get("state", "")),
               action="restart_all",
               result="success" if ok else "failed",
            )
         except Exception:
            _emit_event(
               event_type="recovery_finished",
               severity="critical",
               source="nestdaq-monitor",
               state_before=str(summary.get("state", "")),
               state_after=str(summary.get("state", "")),
               action="restart_all",
               result="failed",
            )


def _monitor_worker():
   while True:
      try:
         _monitor_once()
      except Exception:
         # Keep monitor alive regardless of transient runtime errors.
         _emit_event(
            event_type="monitor_error",
            severity="critical",
            source="nestdaq-monitor",
            state_before="",
            state_after="",
            action="none",
            result="failed",
         )
      sleep_sec = _monitor_interval_sec if _monitor_interval_sec > 0 else 1.0
      time.sleep(sleep_sec)


def _start_monitor_if_needed():
   global _monitor_thread
   if _monitor_thread is None or not _monitor_thread.is_alive():
      _monitor_thread = threading.Thread(target=_monitor_worker, daemon=True)
      _monitor_thread.start()


def _sse_data(event: Dict) -> str:
   payload = json.dumps(event, ensure_ascii=True)
   return f"event: nestdaq\ndata: {payload}\n\n"


def _state_log_select(sql_text: str, params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
   try:
      ready, err = state_log_schema_ready()
      if not ready:
         return [], err
      engine = get_state_log_engine()
      with engine.connect() as conn:
         rows = conn.execute(text(sql_text), params).mappings().all()
      return [dict(r) for r in rows], ""
   except Exception as ex:
      return [], str(ex)


def _refresh_state_log_runtime_meta():
   cfg = state_log_runtime_config()
   with _state_log_lock:
      _state_log_status["backend"] = str(cfg.get("backend", "postgresql"))
      _state_log_status["db_target"] = state_log_database_url()


_load_monitor_config_from_settings()
_start_state_log_writer_if_needed()
_start_monitor_if_needed()

@router.get('/')
async def root() : 
   return {"message": "nestdaq api"}


@router.get('/monitor/config/get')
async def monitor_config_get():
   with _config_lock:
      current = _monitor_config_dict()
   return {
      "message": "ok",
      "current": current,
      "defaults": dict(_monitor_config_defaults),
      "settings_file": get_settings_file_path(),
   }


@router.get('/state-log/config/get')
async def state_log_config_get():
   _refresh_state_log_runtime_meta()
   return {
      "message": "ok",
      "current": state_log_runtime_config(),
      "defaults": state_log_default_config(),
      "settings_file": get_settings_file_path(),
   }


@router.post('/state-log/config/set')
async def state_log_config_set(payload: Dict):
   global _state_log_repository
   if not isinstance(payload, dict):
      raise HTTPException(status_code=400, detail="payload must be an object")

   current = state_log_runtime_config()
   merged = dict(current)
   merged.update(payload)
   normalized = set_state_log_runtime_config(merged)
   _state_log_repository = SQLAlchemyStateLogRepository()
   _refresh_state_log_runtime_meta()

   _emit_event(
      event_type="state_log_config_updated",
      severity="info",
      source="api-request",
      state_before="",
      state_after="",
      action="update_state_log_config",
      result="success",
   )
   return {
      "message": "ok",
      "current": normalized,
      "settings_file": get_settings_file_path(),
   }


@router.post('/monitor/config/set')
async def monitor_config_set(payload: Dict):
   if not isinstance(payload, dict):
      raise HTTPException(status_code=400, detail="payload must be an object")

   with _config_lock:
      base = dict(_monitor_config_defaults)
      base.update(_monitor_config_dict())
      base.update(payload)
      _apply_monitor_config(base)
      current = _monitor_config_dict()
      _save_monitor_config_to_settings(current)

   _emit_event(
      event_type="monitor_config_updated",
      severity="info",
      source="api-request",
      state_before="",
      state_after="",
      action="update_monitor_config",
      result="success",
   )
   return {
      "message": "ok",
      "current": current,
      "settings_file": get_settings_file_path(),
   }

@router.get('/status/')
async def read_status():
   rcli = _rcli()
   status = _collect_status(rcli)
   key_state = status.get("states", {})
   if not key_state:
      return {}
   return key_state


@router.get('/health/summary')
async def health_summary():
   with _health_lock:
      return dict(_last_summary)


@router.get('/health/events')
async def health_events(limit: int = Query(default=100, ge=1, le=1000)):
   with _event_cv:
      return {"events": list(_events)[-limit:]}


@router.get('/state-log/status')
async def state_log_status():
   _refresh_state_log_runtime_meta()
   schema_ready, schema_error = state_log_schema_ready()
   with _state_log_lock:
      status = dict(_state_log_status)
      status["pending_records"] = len(_state_log_queue)
      status["queue_max"] = int(_state_log_queue_max)
      status["retry_sec"] = float(_state_log_retry_sec)
      status["schema_ready"] = bool(schema_ready)
      status["migration_required"] = not schema_ready
      status["schema_error"] = schema_error
   if schema_ready:
      message = "ok"
   elif schema_error.startswith("missing tables:"):
      message = "migration_required"
   else:
      message = "db_unavailable"
   return {"message": message, "status": status}


@router.get('/state-log/transitions')
async def state_log_transitions(
   limit: int = Query(default=200, ge=1, le=5000),
   offset: int = Query(default=0, ge=0),
   from_ts_ms: int = Query(default=0, ge=0),
   to_ts_ms: int = Query(default=4102444800000, ge=0),
   run_number: Optional[int] = Query(default=None),
   transition_type: str = Query(default=""),
):
   where = ["event_ts_ms >= :from_ts_ms", "event_ts_ms <= :to_ts_ms"]
   params: Dict[str, Any] = {"from_ts_ms": int(from_ts_ms), "to_ts_ms": int(to_ts_ms)}
   if run_number is not None:
      where.append("run_number = :run_number")
      params["run_number"] = int(run_number)
   if str(transition_type).strip() != "":
      where.append("transition_type = :transition_type")
      params["transition_type"] = str(transition_type).strip()
   params["limit"] = int(limit)
   params["offset"] = int(offset)
   sql_text = (
      "SELECT * FROM state_transitions WHERE "
      + " AND ".join(where)
      + " ORDER BY event_ts_ms DESC LIMIT :limit OFFSET :offset"
   )
   rows, err = _state_log_select(sql_text, params)
   return {"message": "ok" if err == "" else "db_unavailable", "error": err, "items": rows}


@router.get('/state-log/anomalies')
async def state_log_anomalies(
   limit: int = Query(default=200, ge=1, le=5000),
   offset: int = Query(default=0, ge=0),
   from_ts_ms: int = Query(default=0, ge=0),
   to_ts_ms: int = Query(default=4102444800000, ge=0),
   run_number: Optional[int] = Query(default=None),
   anomaly_type: str = Query(default=""),
   severity: str = Query(default=""),
):
   where = ["event_ts_ms >= :from_ts_ms", "event_ts_ms <= :to_ts_ms"]
   params: Dict[str, Any] = {"from_ts_ms": int(from_ts_ms), "to_ts_ms": int(to_ts_ms)}
   if run_number is not None:
      where.append("run_number = :run_number")
      params["run_number"] = int(run_number)
   if str(anomaly_type).strip() != "":
      where.append("anomaly_type = :anomaly_type")
      params["anomaly_type"] = str(anomaly_type).strip()
   if str(severity).strip() != "":
      where.append("severity = :severity")
      params["severity"] = str(severity).strip()
   params["limit"] = int(limit)
   params["offset"] = int(offset)
   sql_text = (
      "SELECT * FROM anomaly_events WHERE "
      + " AND ".join(where)
      + " ORDER BY event_ts_ms DESC LIMIT :limit OFFSET :offset"
   )
   rows, err = _state_log_select(sql_text, params)
   return {"message": "ok" if err == "" else "db_unavailable", "error": err, "items": rows}


@router.get('/state-log/runs')
async def state_log_runs(
   limit: int = Query(default=200, ge=1, le=5000),
   offset: int = Query(default=0, ge=0),
   from_ts_ms: int = Query(default=0, ge=0),
   to_ts_ms: int = Query(default=4102444800000, ge=0),
   run_number: Optional[int] = Query(default=None),
   status: str = Query(default=""),
):
   where = ["start_ts_ms >= :from_ts_ms", "start_ts_ms <= :to_ts_ms"]
   params: Dict[str, Any] = {"from_ts_ms": int(from_ts_ms), "to_ts_ms": int(to_ts_ms)}
   if run_number is not None:
      where.append("run_number = :run_number")
      params["run_number"] = int(run_number)
   if str(status).strip() != "":
      where.append("status = :status")
      params["status"] = str(status).strip()
   params["limit"] = int(limit)
   params["offset"] = int(offset)
   sql_text = (
      "SELECT * FROM runs WHERE "
      + " AND ".join(where)
      + " ORDER BY start_ts_ms DESC LIMIT :limit OFFSET :offset"
   )
   rows, err = _state_log_select(sql_text, params)
   return {"message": "ok" if err == "" else "db_unavailable", "error": err, "items": rows}


@router.get('/health/events/stream')
async def health_events_stream(since_ts: int = Query(default=0, ge=0)):
   import asyncio
   
   async def generator():
      cursor = int(since_ts)
      while True:
         pending = []
         with _event_cv:
            for event in _events:
               if int(event.get("ts", 0)) > cursor:
                  pending.append(dict(event))
            if not pending:
               _event_cv.wait(timeout=1.0)
               for event in _events:
                  if int(event.get("ts", 0)) > cursor:
                     pending.append(dict(event))

         if not pending:
            heartbeat = {
               "event_type": "heartbeat",
               "severity": "info",
               "source": "nestdaq-monitor",
               "state_before": "",
               "state_after": "",
               "action": "none",
               "result": "success",
               "ts": _now_ts(),
            }
            yield _sse_data(heartbeat)
            await asyncio.sleep(1.0)
            continue

         for event in pending:
            cursor = max(cursor, int(event.get("ts", 0)))
            yield _sse_data(event)
         await asyncio.sleep(0.1)

   return StreamingResponse(
      generator(),
      media_type='text/event-stream',
      headers={
         'Cache-Control': 'no-cache',
         'Connection': 'keep-alive',
         'X-Accel-Buffering': 'no',
      },
   )


@router.post('/recovery/{action}')
async def recovery_action(action: str):
   _emit_event(
      event_type="recovery_started",
      severity="warning",
      source="api-request",
      state_before="",
      state_after="",
      action=action,
      result="running",
   )
   result = _run_recovery_action(action)
   ok = result.get("returncode", 1) == 0
   _emit_event(
      event_type="recovery_finished",
      severity="info" if ok else "critical",
      source="api-request",
      state_before="",
      state_after="",
      action=action,
      result="success" if ok else "failed",
   )
   return result

@router.get("/set_path/{key}/{val:path}")
async def read_item(key: str, val:str) :
   rcli = _rcli()
   rcli.set(key,val)
   return {"message": "set_path"}

@router.get('/run_number')
async def read_run_number():
   rcli = _rcli()
   val_run_number = rcli.get("run_info:run_number")
   return {"message" : _to_text(val_run_number)}

@router.get('/run_comment')
async def read_run_comment():
   rcli = _rcli()
   val_run_comment = rcli.get("run_info:run_comment")
   return {"message" : _to_text(val_run_comment)}
