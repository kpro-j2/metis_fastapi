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
