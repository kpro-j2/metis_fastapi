import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, Optional, Protocol

from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from modules.state_log_db import get_state_log_session_factory, state_log_schema_ready
from modules.state_log_models import AnomalyEvent, RunRecord, StateTransition


RUN_STATUS_RUNNING = "running"
RUN_STATUS_ENDED = "ended"
RUN_STATUS_ABNORMAL_END = "abnormal_end"
RUN_STATUS_ABNORMAL_RUNNING = "abnormal_running"
RUN_STATUS_ABNORMAL_STARTED_ENDED = "abnormal_started_ended"


class StateLogRepositoryError(RuntimeError):
   pass


@dataclass(frozen=True)
class TransitionPayload:
   event_ts_ms: int
   transition_type: str
   from_state: str
   to_state: str
   run_number: Optional[int]
   source: str
   message: str = ""
   detail: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AnomalyPayload:
   event_ts_ms: int
   anomaly_type: str
   severity: str
   state: str
   run_number: Optional[int]
   source: str
   message: str = ""
   detail: Dict[str, Any] = field(default_factory=dict)


class StateLogRepository(Protocol):
   def check_schema_ready(self) -> None:
      ...

   def record_transition(self, payload: TransitionPayload) -> None:
      ...

   def record_anomaly(self, payload: AnomalyPayload) -> None:
      ...


class SQLAlchemyStateLogRepository:
   def __init__(self, session_factory=None):
      self._session_factory = session_factory or get_state_log_session_factory()

   def check_schema_ready(self) -> None:
      ok, err = state_log_schema_ready()
      if not ok:
         raise StateLogRepositoryError(err)

   @contextmanager
   def _session_scope(self) -> Iterator[Session]:
      session = self._session_factory()
      try:
         yield session
         session.commit()
      except SQLAlchemyError as ex:
         session.rollback()
         raise StateLogRepositoryError(str(ex)) from ex
      finally:
         session.close()

   def record_transition(self, payload: TransitionPayload) -> None:
      transition = StateTransition(
         event_ts_ms=int(payload.event_ts_ms),
         recorded_ts_ms=int(payload.event_ts_ms),
         transition_type=str(payload.transition_type),
         from_state=str(payload.from_state),
         to_state=str(payload.to_state),
         run_number=payload.run_number,
         source=str(payload.source),
         message=str(payload.message),
         detail_json=json.dumps(payload.detail, ensure_ascii=False),
      )

      with self._session_scope() as session:
         session.add(transition)
         if payload.transition_type == "run_start":
            # If another run is still open, close it as abnormal_end before starting a new run.
            open_rows = session.execute(
               select(RunRecord)
               .where(RunRecord.status.in_([RUN_STATUS_RUNNING, RUN_STATUS_ABNORMAL_RUNNING]))
               .order_by(RunRecord.run_id.asc())
            ).scalars().all()

            for row in open_rows:
               row.end_ts_ms = int(payload.event_ts_ms)
               row.end_state = str(payload.from_state)
               row.status = RUN_STATUS_ABNORMAL_END
               row.end_detail_json = json.dumps(
                  {
                     "reason": "new_run_started_before_previous_end",
                     "next_run_number": payload.run_number,
                  },
                  ensure_ascii=False,
               )
               row.updated_ts_ms = int(payload.event_ts_ms)

            new_run_status = RUN_STATUS_ABNORMAL_RUNNING if len(open_rows) > 0 else RUN_STATUS_RUNNING
            session.add(
               RunRecord(
                  run_number=payload.run_number,
                  start_ts_ms=int(payload.event_ts_ms),
                  end_ts_ms=None,
                  start_state=str(payload.to_state),
                  end_state=None,
                  status=new_run_status,
                  start_detail_json=json.dumps(payload.detail, ensure_ascii=False),
                  end_detail_json=None,
                  updated_ts_ms=int(payload.event_ts_ms),
               )
            )
         elif payload.transition_type == "run_end":
            open_query = (
               select(RunRecord)
               .where(RunRecord.status.in_([RUN_STATUS_RUNNING, RUN_STATUS_ABNORMAL_RUNNING]))
               .order_by(RunRecord.run_id.desc())
            )

            row = None
            if payload.run_number is not None:
               row = session.execute(
                  open_query.where(RunRecord.run_number == payload.run_number).limit(1)
               ).scalar_one_or_none()
            if row is None:
               row = session.execute(open_query.limit(1)).scalar_one_or_none()

            if row is None:
               session.add(
                  RunRecord(
                     run_number=payload.run_number,
                     start_ts_ms=int(payload.event_ts_ms),
                     end_ts_ms=int(payload.event_ts_ms),
                     start_state=str(payload.from_state),
                     end_state=str(payload.to_state),
                     status=RUN_STATUS_ABNORMAL_STARTED_ENDED,
                     start_detail_json=json.dumps(
                        {
                           **payload.detail,
                           "reason": "run_end_without_run_start",
                        },
                        ensure_ascii=False,
                     ),
                     end_detail_json=json.dumps(
                        {
                           **payload.detail,
                           "reason": "run_end_without_run_start",
                        },
                        ensure_ascii=False,
                     ),
                     updated_ts_ms=int(payload.event_ts_ms),
                  )
               )
            else:
               row.end_ts_ms = int(payload.event_ts_ms)
               row.end_state = str(payload.to_state)
               row.status = (
                  RUN_STATUS_ABNORMAL_STARTED_ENDED
                  if row.status == RUN_STATUS_ABNORMAL_RUNNING
                  else RUN_STATUS_ENDED
               )
               row.end_detail_json = json.dumps(payload.detail, ensure_ascii=False)
               row.updated_ts_ms = int(payload.event_ts_ms)

   def record_anomaly(self, payload: AnomalyPayload) -> None:
      with self._session_scope() as session:
         session.add(
            AnomalyEvent(
               event_ts_ms=int(payload.event_ts_ms),
               recorded_ts_ms=int(payload.event_ts_ms),
               anomaly_type=str(payload.anomaly_type),
               severity=str(payload.severity),
               state=str(payload.state),
               run_number=payload.run_number,
               source=str(payload.source),
               message=str(payload.message),
               detail_json=json.dumps(payload.detail, ensure_ascii=False),
            )
         )