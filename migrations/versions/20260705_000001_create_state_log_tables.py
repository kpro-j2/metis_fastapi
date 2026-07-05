"""create state log tables

Revision ID: 20260705_000001
Revises: 
Create Date: 2026-07-05 18:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260705_000001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
   op.create_table(
      "state_transitions",
      sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
      sa.Column("event_ts_ms", sa.BigInteger(), nullable=False),
      sa.Column("recorded_ts_ms", sa.BigInteger(), nullable=False),
      sa.Column("transition_type", sa.String(length=64), nullable=False),
      sa.Column("from_state", sa.String(length=64), nullable=False),
      sa.Column("to_state", sa.String(length=64), nullable=False),
      sa.Column("run_number", sa.Integer(), nullable=True),
      sa.Column("source", sa.String(length=64), nullable=False),
      sa.Column("message", sa.Text(), nullable=False, server_default=""),
      sa.Column("detail_json", sa.Text(), nullable=False, server_default="{}"),
   )
   op.create_index("idx_state_transitions_event_ts", "state_transitions", ["event_ts_ms"])
   op.create_index("idx_state_transitions_run_number", "state_transitions", ["run_number"])
   op.create_index("idx_state_transitions_transition_type", "state_transitions", ["transition_type"])

   op.create_table(
      "anomaly_events",
      sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
      sa.Column("event_ts_ms", sa.BigInteger(), nullable=False),
      sa.Column("recorded_ts_ms", sa.BigInteger(), nullable=False),
      sa.Column("anomaly_type", sa.String(length=64), nullable=False),
      sa.Column("severity", sa.String(length=32), nullable=False),
      sa.Column("state", sa.String(length=64), nullable=False),
      sa.Column("run_number", sa.Integer(), nullable=True),
      sa.Column("source", sa.String(length=64), nullable=False),
      sa.Column("message", sa.Text(), nullable=False, server_default=""),
      sa.Column("detail_json", sa.Text(), nullable=False, server_default="{}"),
   )
   op.create_index("idx_anomaly_events_event_ts", "anomaly_events", ["event_ts_ms"])
   op.create_index("idx_anomaly_events_run_number", "anomaly_events", ["run_number"])
   op.create_index("idx_anomaly_events_type", "anomaly_events", ["anomaly_type"])
   op.create_index("idx_anomaly_events_severity", "anomaly_events", ["severity"])

   op.create_table(
      "runs",
      sa.Column("run_id", sa.Integer(), primary_key=True, autoincrement=True),
      sa.Column("run_number", sa.Integer(), nullable=True),
      sa.Column("start_ts_ms", sa.BigInteger(), nullable=False),
      sa.Column("end_ts_ms", sa.BigInteger(), nullable=True),
      sa.Column("start_state", sa.String(length=64), nullable=False),
      sa.Column("end_state", sa.String(length=64), nullable=True),
      sa.Column("status", sa.String(length=32), nullable=False),
      sa.Column("start_detail_json", sa.Text(), nullable=False, server_default="{}"),
      sa.Column("end_detail_json", sa.Text(), nullable=True),
      sa.Column("updated_ts_ms", sa.BigInteger(), nullable=False),
   )
   op.create_index("idx_runs_run_number", "runs", ["run_number"])
   op.create_index("idx_runs_start_ts", "runs", ["start_ts_ms"])
   op.create_index("idx_runs_status", "runs", ["status"])


def downgrade() -> None:
   op.drop_index("idx_runs_status", table_name="runs")
   op.drop_index("idx_runs_start_ts", table_name="runs")
   op.drop_index("idx_runs_run_number", table_name="runs")
   op.drop_table("runs")

   op.drop_index("idx_anomaly_events_severity", table_name="anomaly_events")
   op.drop_index("idx_anomaly_events_type", table_name="anomaly_events")
   op.drop_index("idx_anomaly_events_run_number", table_name="anomaly_events")
   op.drop_index("idx_anomaly_events_event_ts", table_name="anomaly_events")
   op.drop_table("anomaly_events")

   op.drop_index("idx_state_transitions_transition_type", table_name="state_transitions")
   op.drop_index("idx_state_transitions_run_number", table_name="state_transitions")
   op.drop_index("idx_state_transitions_event_ts", table_name="state_transitions")
   op.drop_table("state_transitions")
