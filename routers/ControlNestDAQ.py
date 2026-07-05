import json
import os
import subprocess
import threading
import time
from collections import deque
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from metis_fastapi.dependencies import get_redis_proxy
router = APIRouter(
   prefix="/nestdaq",
   tags=["nestdaq"]
)


aProxy = get_redis_proxy(0)

_health_lock = threading.Lock()
_event_cv = threading.Condition()
_monitor_thread: Optional[threading.Thread] = None
_monitor_interval_sec = float(os.getenv("NESTDAQ_MONITOR_INTERVAL_SEC", "1.0"))
_stale_threshold_sec = float(os.getenv("NESTDAQ_STALE_THRESHOLD_SEC", "5.0"))
_event_buffer_max = int(os.getenv("NESTDAQ_EVENT_BUFFER_MAX", "500"))
_recovery_cooldown_sec = float(os.getenv("NESTDAQ_RECOVERY_COOLDOWN_SEC", "60.0"))
_recovery_timeout_sec = int(os.getenv("NESTDAQ_RECOVERY_TIMEOUT_SEC", "120"))
_auto_recovery_enabled = os.getenv("NESTDAQ_AUTO_RECOVERY", "0") == "1"
_recovery_script = os.getenv(
   "NESTDAQ_RECOVERY_SCRIPT",
   "/home/akeno/repos/workdir-akeno-spadi/scripts/run-nestdaq-akeno.sh",
)

_events = deque(maxlen=_event_buffer_max)
_last_summary = {
   "overall": "unknown",
   "service_count": 0,
   "mismatch": False,
   "stale_services": [],
   "state": "",
   "states": {},
   "updated": {},
   "last_checked_ts": 0,
   "auto_recovery_enabled": _auto_recovery_enabled,
}
_last_recovery_ts = 0.0
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

   return {
      "overall": overall,
      "service_count": len(states),
      "mismatch": mismatch,
      "stale_services": sorted(stale_services),
      "state": merged_state,
      "states": states,
      "updated": updated,
      "last_checked_ts": _now_ts(),
      "auto_recovery_enabled": _auto_recovery_enabled,
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


def _monitor_once():
   global _last_summary
   global _last_recovery_ts

   before = _last_summary
   rcli = aProxy.instance()
   if rcli is None:
      summary = {
         "overall": "critical",
         "service_count": 0,
         "mismatch": False,
         "stale_services": [],
         "state": "NO REDIS",
         "states": {},
         "updated": {},
         "last_checked_ts": _now_ts(),
         "auto_recovery_enabled": _auto_recovery_enabled,
      }
   else:
      summary = _evaluate_summary(_collect_status(rcli))

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
      _emit_event(
         event_type="service_stale_detected",
         severity="critical",
         source="nestdaq-monitor",
         state_before=str(before.get("state", "")),
         state_after=str(summary.get("state", "")),
         action="none",
         result="success",
      )

   if _auto_recovery_enabled and summary.get("overall") == "critical":
      now = time.time()
      if now - _last_recovery_ts >= _recovery_cooldown_sec:
         _last_recovery_ts = now
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


_start_monitor_if_needed()

@router.get('/')
async def root() : 
   return {"message": "nestdaq api"}

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
