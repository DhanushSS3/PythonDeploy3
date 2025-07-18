"""Add crypto table

Revision ID: ac9ae41fd5c5
Revises: e09979cf978f
Create Date: 2025-07-07 03:09:03.132749

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ac9ae41fd5c5'
down_revision: Union[str, None] = 'e09979cf978f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(op.f('ix_demo_user_orders_cancel_id'), 'demo_user_orders', ['cancel_id'], unique=False)
    op.create_index(op.f('ix_demo_user_orders_close_id'), 'demo_user_orders', ['close_id'], unique=False)
    op.create_index(op.f('ix_demo_user_orders_modify_id'), 'demo_user_orders', ['modify_id'], unique=False)
    op.create_index(op.f('ix_demo_user_orders_stoploss_cancel_id'), 'demo_user_orders', ['stoploss_cancel_id'], unique=False)
    op.create_index(op.f('ix_demo_user_orders_stoploss_id'), 'demo_user_orders', ['stoploss_id'], unique=False)
    op.create_index(op.f('ix_demo_user_orders_takeprofit_cancel_id'), 'demo_user_orders', ['takeprofit_cancel_id'], unique=False)
    op.create_index(op.f('ix_demo_user_orders_takeprofit_id'), 'demo_user_orders', ['takeprofit_id'], unique=False)
    op.create_index(op.f('ix_user_orders_cancel_id'), 'user_orders', ['cancel_id'], unique=False)
    op.create_index(op.f('ix_user_orders_close_id'), 'user_orders', ['close_id'], unique=False)
    op.create_index(op.f('ix_user_orders_modify_id'), 'user_orders', ['modify_id'], unique=False)
    op.create_index(op.f('ix_user_orders_stoploss_cancel_id'), 'user_orders', ['stoploss_cancel_id'], unique=False)
    op.create_index(op.f('ix_user_orders_stoploss_id'), 'user_orders', ['stoploss_id'], unique=False)
    op.create_index(op.f('ix_user_orders_takeprofit_cancel_id'), 'user_orders', ['takeprofit_cancel_id'], unique=False)
    op.create_index(op.f('ix_user_orders_takeprofit_id'), 'user_orders', ['takeprofit_id'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_user_orders_takeprofit_id'), table_name='user_orders')
    op.drop_index(op.f('ix_user_orders_takeprofit_cancel_id'), table_name='user_orders')
    op.drop_index(op.f('ix_user_orders_stoploss_id'), table_name='user_orders')
    op.drop_index(op.f('ix_user_orders_stoploss_cancel_id'), table_name='user_orders')
    op.drop_index(op.f('ix_user_orders_modify_id'), table_name='user_orders')
    op.drop_index(op.f('ix_user_orders_close_id'), table_name='user_orders')
    op.drop_index(op.f('ix_user_orders_cancel_id'), table_name='user_orders')
    op.drop_index(op.f('ix_demo_user_orders_takeprofit_id'), table_name='demo_user_orders')
    op.drop_index(op.f('ix_demo_user_orders_takeprofit_cancel_id'), table_name='demo_user_orders')
    op.drop_index(op.f('ix_demo_user_orders_stoploss_id'), table_name='demo_user_orders')
    op.drop_index(op.f('ix_demo_user_orders_stoploss_cancel_id'), table_name='demo_user_orders')
    op.drop_index(op.f('ix_demo_user_orders_modify_id'), table_name='demo_user_orders')
    op.drop_index(op.f('ix_demo_user_orders_close_id'), table_name='demo_user_orders')
    op.drop_index(op.f('ix_demo_user_orders_cancel_id'), table_name='demo_user_orders')
    # ### end Alembic commands ###
