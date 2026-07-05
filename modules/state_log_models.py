from sqlalchemy import BigInteger, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from modules.state_log_db import Base


class StateTransition(Base):
   __tablename__ = "state_transitions"

   id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
   event_ts_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
   recorded_ts_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
   transition_type: Mapped[str] = mapped_column(String(64), nullable=False)
   from_state: Mapped[str] = mapped_column(String(64), nullable=False)
   to_state: Mapped[str] = mapped_column(String(64), nullable=False)
   run_number: Mapped[int] = mapped_column(Integer, nullable=True)
   source: Mapped[str] = mapped_column(String(64), nullable=False)
   message: Mapped[str] = mapped_column(Text, nullable=False, default="")
   detail_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

   __table_args__ = (
      Index("idx_state_transitions_event_ts", "event_ts_ms"),
      Index("idx_state_transitions_run_number", "run_number"),
      Index("idx_state_transitions_transition_type", "transition_type"),
   )


class AnomalyEvent(Base):
   __tablename__ = "anomaly_events"

   id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
   event_ts_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
   recorded_ts_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
   anomaly_type: Mapped[str] = mapped_column(String(64), nullable=False)
   severity: Mapped[str] = mapped_column(String(32), nullable=False)
   state: Mapped[str] = mapped_column(String(64), nullable=False)
   run_number: Mapped[int] = mapped_column(Integer, nullable=True)
   source: Mapped[str] = mapped_column(String(64), nullable=False)
   message: Mapped[str] = mapped_column(Text, nullable=False, default="")
   detail_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

   __table_args__ = (
      Index("idx_anomaly_events_event_ts", "event_ts_ms"),
      Index("idx_anomaly_events_run_number", "run_number"),
      Index("idx_anomaly_events_type", "anomaly_type"),
      Index("idx_anomaly_events_severity", "severity"),
   )


class RunRecord(Base):
   __tablename__ = "runs"

   run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
   run_number: Mapped[int] = mapped_column(Integer, nullable=True)
   start_ts_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
   end_ts_ms: Mapped[int] = mapped_column(BigInteger, nullable=True)
   start_state: Mapped[str] = mapped_column(String(64), nullable=False)
   end_state: Mapped[str] = mapped_column(String(64), nullable=True)
   status: Mapped[str] = mapped_column(String(32), nullable=False)
   start_detail_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
   end_detail_json: Mapped[str] = mapped_column(Text, nullable=True)
   updated_ts_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)

   __table_args__ = (
      Index("idx_runs_run_number", "run_number"),
      Index("idx_runs_start_ts", "start_ts_ms"),
      Index("idx_runs_status", "status"),
   )