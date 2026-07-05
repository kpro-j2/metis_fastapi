"""add run telemetry columns

Revision ID: 20260705_000002
Revises: 20260705_000001
Create Date: 2026-07-05 23:20:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260705_000002"
down_revision = "20260705_000001"
branch_labels = None
depends_on = None


def upgrade() -> None:
   op.add_column("runs", sa.Column("start_scaler_json", sa.Text(), nullable=False, server_default="{}"))
   op.add_column("runs", sa.Column("start_hv_json", sa.Text(), nullable=False, server_default="{}"))
   op.add_column("runs", sa.Column("end_scaler_json", sa.Text(), nullable=True))
   op.add_column("runs", sa.Column("end_hv_json", sa.Text(), nullable=True))
   op.alter_column("runs", "start_scaler_json", server_default=None)
   op.alter_column("runs", "start_hv_json", server_default=None)


def downgrade() -> None:
   op.drop_column("runs", "end_hv_json")
   op.drop_column("runs", "end_scaler_json")
   op.drop_column("runs", "start_hv_json")
   op.drop_column("runs", "start_scaler_json")
