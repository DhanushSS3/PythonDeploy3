# app/api/v1/endpoints/market_data_ws.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status, Query, WebSocketException, Depends, HTTPException
import asyncio
import logging
from app.core.logging_config import websocket_logger, orders_logger

from app.crud.crud_order import get_order_model
import json
# import threading # No longer needed for active_connections_lock
from typing import Dict, Any, List, Optional, Set
import decimal
from starlette.websockets import WebSocketState
from decimal import Decimal
import datetime
import random


# Import necessary components for DB interaction and authentication
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db, AsyncSessionLocal # Import AsyncSessionLocal for db sessions in tasks
from app.crud.user import get_user_by_account_number, get_demo_user_by_account_number
from app.crud import group as crud_group
from app.crud import crud_order

# Import security functions for token validation
from app.core.security import decode_token

# Import the Redis client type
from redis.asyncio import Redis

# Import the caching helper functions
from app.core.cache import (
    set_user_data_cache, get_user_data_cache,
    set_user_portfolio_cache, get_user_portfolio_cache,
    # get_user_positions_from_cache, # Will be part of get_user_portfolio_cache
    set_adjusted_market_price_cache, get_adjusted_market_price_cache,
    set_group_symbol_settings_cache, get_group_symbol_settings_cache,
    set_last_known_price, get_last_known_price,  # <-- For last known price caching
    # New cache functions
    set_user_static_orders_cache, get_user_static_orders_cache,
    set_user_dynamic_portfolio_cache, get_user_dynamic_portfolio_cache,
    set_user_balance_margin_cache, get_user_balance_margin_cache,
    DecimalEncoder, decode_decimal,
    # Redis channels
    REDIS_MARKET_DATA_CHANNEL,
    REDIS_ORDER_UPDATES_CHANNEL,
    REDIS_USER_DATA_UPDATES_CHANNEL
)

# Import the dependency to get the Redis client
from app.dependencies.redis_client import get_redis_client

# Import the shared state for the Redis publish queue (for Firebase data -> Redis)
from app.shared_state import redis_publish_queue
from app.shared_state import adjusted_prices_in_memory

# Import the new portfolio calculation service
from app.services.portfolio_calculator import calculate_user_portfolio

# Import the Symbol and ExternalSymbolInfo models
from app.database.models import Symbol, ExternalSymbolInfo, User, DemoUser # Import User/DemoUser for type hints
from sqlalchemy.future import select
from app.services.pending_orders import process_order_stoploss_takeprofit
from app.crud.crud_order import get_all_open_orders_by_user_id, get_order_model
from app.database.models import UserOrder, DemoUserOrder

# Configure logging for this module
logger = websocket_logger

# REMOVE: active_websocket_connections and active_connections_lock
# active_websocket_connections: Dict[int, Dict[str, Any]] = {}
# active_connections_lock = threading.Lock() 

# Redis channel for RAW market data updates from Firebase via redis_publisher_task
REDIS_MARKET_DATA_CHANNEL = 'market_data_updates'

# Add global connection tracking for monitoring
import threading
from collections import defaultdict
import time

# Global connection tracking
active_websocket_connections = {}
connection_metrics = {
    'total_connections': 0,
    'current_connections': 0,
    'max_concurrent_connections': 0,
    'connection_times': [],
    'failed_connections': 0,
    'last_connection_time': None
}
metrics_lock = threading.Lock()

def update_connection_metrics(connection_id: str, connection_time: float, success: bool):
    """Update connection metrics"""
    with metrics_lock:
        connection_metrics['total_connections'] += 1
        if success:
            connection_metrics['current_connections'] += 1
            connection_metrics['connection_times'].append(connection_time)
            connection_metrics['max_concurrent_connections'] = max(
                connection_metrics['max_concurrent_connections'],
                connection_metrics['current_connections']
            )
            connection_metrics['last_connection_time'] = time.time()
        else:
            connection_metrics['failed_connections'] += 1

def remove_connection_metrics():
    """Remove connection from metrics when disconnected"""
    with metrics_lock:
        connection_metrics['current_connections'] = max(0, connection_metrics['current_connections'] - 1)

async def safe_websocket_send(websocket: WebSocket, message: str, user_id: int, context: str = "message"):
    """
    Safely send a message to a WebSocket connection with proper error handling.
    Returns True if successful, False if connection is closed.
    """
    try:
        if websocket.client_state == WebSocketState.CONNECTED:
            await websocket.send_text(message)
            logger.debug(f"User {user_id}: Successfully sent {context}")
            return True
        else:
            logger.debug(f"User {user_id}: WebSocket not connected, skipping {context}")
            return False
    except WebSocketDisconnect:
        logger.info(f"User {user_id}: WebSocket disconnected while sending {context}")
        return False
    except Exception as send_error:
        logger.error(f"User {user_id}: Error sending {context}: {send_error}")
        # Check if it's a connection-related error
        if "ConnectionClosedOK" in str(send_error) or "ClientDisconnected" in str(send_error):
            logger.info(f"User {user_id}: WebSocket connection closed, stopping {context}")
            return False
        return False

router = APIRouter(
    tags=["market_data"]
)


async def _get_full_portfolio_details(
    user_id: int,
    group_name: str, # User's group name
    redis_client: Redis,
    db: AsyncSession,
    user_type: str
) -> Optional[Dict[str, Any]]:
    """
    Fetches all necessary data from cache and calculates the full user portfolio.
    """
    user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
    user_portfolio_cache = await get_user_portfolio_cache(redis_client, user_id) # Contains positions
    group_symbol_settings_all = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")

    if not user_data or not group_symbol_settings_all:
        logger.warning(f"Missing user_data or group_settings for user {user_id}, group {group_name}. Cannot calculate portfolio.")
        return None

    open_positions = user_portfolio_cache.get('positions', []) if user_portfolio_cache else []
    
    # Construct the market_prices dict for calculate_user_portfolio using cached adjusted prices
    market_prices_for_calc = {}
    relevant_symbols_for_group = set(group_symbol_settings_all.keys())
    for sym_upper in relevant_symbols_for_group:
        cached_adj_price = await get_adjusted_market_price_cache(redis_client, group_name, sym_upper)
        if cached_adj_price:
            # calculate_user_portfolio expects 'buy' and 'sell' keys
            market_prices_for_calc[sym_upper] = {
                'buy': Decimal(str(cached_adj_price.get('buy'))),
                'sell': Decimal(str(cached_adj_price.get('sell')))
            }

    portfolio_metrics = await calculate_user_portfolio(
        user_data=user_data, # This is a dict
        open_positions=open_positions, # List of dicts
        adjusted_market_prices=market_prices_for_calc, # Dict of symbol -> {'buy': Decimal, 'sell': Decimal}
        group_symbol_settings=group_symbol_settings_all, # Dict of symbol -> settings dict
        redis_client=redis_client
    )
    
    # Ensure all values in portfolio_metrics are JSON serializable
    account_data_payload = {
        "balance": portfolio_metrics.get("balance", "0.0"),
        "equity": portfolio_metrics.get("equity", "0.0"),
        "margin": user_data.get("margin", "0.0"), # User's OVERALL margin from cached user_data
        "free_margin": portfolio_metrics.get("free_margin", "0.0"),
        "profit_loss": portfolio_metrics.get("profit_loss", "0.0"),
        "margin_level": portfolio_metrics.get("margin_level", "0.0"),
        "positions": portfolio_metrics.get("positions", []) # This should be serializable list of dicts
    }
    return account_data_payload


async def check_and_trigger_pending_orders(redis_client, db, symbol, adjusted_prices, group_name):
    """
    Check if any pending orders should be triggered based on current market prices.
    This is called when market data updates are received.
    """
    try:
        # Get all pending orders for this symbol from Redis
        redis_keys = [
            f"pending_orders:{symbol}:BUY_LIMIT",
            f"pending_orders:{symbol}:SELL_LIMIT",
            f"pending_orders:{symbol}:BUY_STOP",
            f"pending_orders:{symbol}:SELL_STOP"
        ]
        
        adjusted_buy_price = adjusted_prices.get('buy')
        if not adjusted_buy_price:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION] Adjusted buy price missing for symbol {symbol} in check_and_trigger_pending_orders. Skipping all pending orders for this symbol.")
        # Check each order type key
        for redis_key in redis_keys:
            try:
                # Get all user orders for this key
                all_user_orders = await redis_client.hgetall(redis_key)
                if not all_user_orders:
                    continue
                
                order_type = redis_key.split(":")[-1]
                logger.debug(f"Checking {len(all_user_orders)} users with {order_type} orders for symbol {symbol}")
                
                # Process each user's orders
                for user_id_bytes, orders_json in all_user_orders.items():
                    user_id = user_id_bytes
                    orders_list = json.loads(orders_json)
                    for order in orders_list:
                        # Normalize decimal values for comparison - round to 5 decimal places
                        try:
                            order_price = Decimal(str(order.get('order_price', '0')))
                            adjusted_buy_price_str = str(adjusted_buy_price)
                            
                            # Ensure the adjusted_buy_price has at least 5 decimal places
                            if '.' in adjusted_buy_price_str:
                                integer_part, decimal_part = adjusted_buy_price_str.split('.')
                                if len(decimal_part) < 5:
                                    decimal_part = decimal_part.ljust(5, '0')
                                adjusted_buy_price_str = f"{integer_part}.{decimal_part}"
                            
                            # Round to 5 decimal places for consistent comparison
                            order_price_normalized = Decimal(str(round(order_price, 5)))
                            adjusted_buy_price_normalized = Decimal(str(round(Decimal(adjusted_buy_price_str), 5)))
                            
                            # Log the raw values
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Raw values - order_price: {order_price}, adjusted_buy_price: {adjusted_buy_price}")
                        except Exception as e:
                            orders_logger.error(f"[PENDING_ORDER_EXECUTION] Error normalizing decimal values: {str(e)}", exc_info=True)
                            continue
                        
                        should_trigger = False
                        if order_type in ['BUY_LIMIT', 'SELL_STOP']:
                            should_trigger = adjusted_buy_price_normalized <= order_price_normalized
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] BUY_LIMIT/SELL_STOP check: {adjusted_buy_price_normalized} <= {order_price_normalized} = {should_trigger}")
                        elif order_type in ['SELL_LIMIT', 'BUY_STOP']:
                            should_trigger = adjusted_buy_price_normalized >= order_price_normalized
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] SELL_LIMIT/BUY_STOP check: {adjusted_buy_price_normalized} >= {order_price_normalized} = {should_trigger}")
                        else:
                            # orders_logger.error(f"[PENDING_ORDER_EXECUTION] Unknown order type {order_type} for order {order.get('order_id')}. Skipping.")
                            continue
                        
                        # Additional debug logging for price comparison
                        # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Price comparison values - adjusted_buy_price_normalized: {adjusted_buy_price_normalized} ({type(adjusted_buy_price_normalized)}), order_price_normalized: {order_price_normalized} ({type(order_price_normalized)})")
                        
                        # Compare as strings for consistent comparison
                        adjusted_price_str = str(adjusted_buy_price_normalized)
                        order_price_str = str(order_price_normalized)
                        # orders_logger.info(f"[PENDING_ORDER_EXECUTION] String comparison for prices: '{adjusted_price_str}' vs '{order_price_str}'")
                        
                        # Compare numeric difference
                        price_diff = abs(adjusted_buy_price_normalized - order_price_normalized)
                        # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Absolute price difference: {price_diff}")
                        
                        # Compare with small epsilon tolerance to catch very close values
                        epsilon = Decimal('0.00001')  # Small tolerance
                        is_close = price_diff < epsilon
                        # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Prices within epsilon tolerance: {is_close} (epsilon={epsilon})")
                        
                        # Consider using epsilon for near-exact matches
                        should_trigger_with_epsilon = False
                        if order_type in ['BUY_LIMIT', 'SELL_STOP']:
                            should_trigger_with_epsilon = (adjusted_buy_price_normalized <= order_price_normalized) or is_close
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] BUY_LIMIT/SELL_STOP with epsilon: {should_trigger_with_epsilon}")
                        elif order_type in ['SELL_LIMIT', 'BUY_STOP']:
                            should_trigger_with_epsilon = (adjusted_buy_price_normalized >= order_price_normalized) or is_close
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] SELL_LIMIT/BUY_STOP with epsilon: {should_trigger_with_epsilon}")
                        
                        # Use the epsilon-based trigger when prices are very close
                        if should_trigger_with_epsilon and not should_trigger:
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Using epsilon-based trigger since prices are very close")
                            should_trigger = True
                        
                        # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Checking order {order.get('order_id')}: type={order_type}, adjusted_buy_price={adjusted_buy_price_normalized}, order_price={order_price_normalized}, should_trigger={should_trigger}")
                        if should_trigger:
                            # orders_logger.info(f"[PENDING_ORDER_EXECUTION] Trigger condition met for order {order.get('order_id')}. Executing trigger_pending_order.")
                            from app.services.pending_orders import trigger_pending_order
                            # Use a new database session for trigger_pending_order to ensure fresh data
                            from app.database.session import AsyncSessionLocal
                            async with AsyncSessionLocal() as trigger_db:
                                from app.services.pending_orders import trigger_pending_order
                                await trigger_pending_order(
                                    db=trigger_db,
                                    redis_client=redis_client,
                                    order=order,
                                    current_price=adjusted_buy_price_normalized
                                )
                        # else:
                        #     orders_logger.info(f"[PENDING_ORDER_EXECUTION] Order {order.get('order_id')} conditions not met for execution. Skipping.")
            
            except Exception as e:
                logger.error(f"Error processing pending orders for key {redis_key}: {e}", exc_info=True)
    
    except Exception as e:
        logger.error(f"Error in check_and_trigger_pending_orders for symbol {symbol}: {e}", exc_info=True)


async def check_and_trigger_sl_tp_orders(redis_client, db, symbol, adjusted_prices, group_name):
    """
    For each open order (live and demo) for the symbol, check if SL/TP should trigger using adjusted prices.
    Always fetches latest open orders from DB and refreshes each order before checking.
    """
    from app.database.models import User, DemoUser, UserOrder, DemoUserOrder
    from app.services.pending_orders import process_order_stoploss_takeprofit
    from sqlalchemy.future import select
    import logging
    logger = logging.getLogger("orders")

    # LIVE ORDERS
    live_open_orders = (await db.execute(
        select(UserOrder).join(User).where(
            UserOrder.order_company_name == symbol,
            UserOrder.order_status == 'OPEN',
            User.group_name == group_name
        )
    )).scalars().all()
    for order in live_open_orders:
        await db.refresh(order)
        if order.order_status != 'OPEN':
            logger.info(f"[SLTP_FLOW] Skipping order {order.order_id} as it is not OPEN (status={order.order_status})")
            continue
        logger.info(f"[SLTP_FLOW] After refresh: order {order.order_id} stop_loss={order.stop_loss}, take_profit={order.take_profit}")
        await process_order_stoploss_takeprofit(db, redis_client, order, user_type='live')

    # DEMO ORDERS
    demo_open_orders = (await db.execute(
        select(DemoUserOrder).join(DemoUser).where(
            DemoUserOrder.order_company_name == symbol,
            DemoUserOrder.order_status == 'OPEN',
            DemoUser.group_name == group_name
        )
    )).scalars().all()
    for order in demo_open_orders:
        await db.refresh(order)
        if order.order_status != 'OPEN':
            logger.info(f"[SLTP_FLOW] Skipping order {order.order_id} as it is not OPEN (status={order.order_status})")
            continue
        logger.info(f"[SLTP_FLOW] After refresh: order {order.order_id} stop_loss={order.stop_loss}, take_profit={order.take_profit}")
        await process_order_stoploss_takeprofit(db, redis_client, order, user_type='demo')


async def per_connection_redis_listener(
    websocket: WebSocket,
    user_id: int,
    group_name: str,
    redis_client: Redis,
    db: AsyncSession,
    user_type: str
):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
    await pubsub.subscribe(REDIS_ORDER_UPDATES_CHANNEL)
    await pubsub.subscribe(REDIS_USER_DATA_UPDATES_CHANNEL)

    is_initial_connection = True
    last_sent_prices = {}

    # --- Optimization: Fetch group settings and symbol list once ---
    group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
    if not group_settings:
        await websocket.close()
        return
    symbol_list = list(group_settings.keys())

    try:
        while websocket.client_state == WebSocketState.CONNECTED:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message is None:
                await asyncio.sleep(0.01)
                continue
            try:
                message_data = json.loads(message['data'], object_hook=decode_decimal)
                channel = message['channel'].decode('utf-8') if isinstance(message['channel'], bytes) else message['channel']

                # --- Always fetch fresh prices from in-memory dict before sending any message ---
                adjusted_prices = adjusted_prices_in_memory.get(group_name, {})

                if channel == REDIS_MARKET_DATA_CHANNEL and message_data.get("type") == "market_data_update":
                    # Only send changed symbols after initial connection
                    changed_prices = {}
                    for symbol in symbol_list:
                        price = adjusted_prices.get(symbol)
                        last = last_sent_prices.get(symbol)
                        if price and (not last or (
                            price['buy'] != last['buy'] or
                            price['sell'] != last['sell'] or
                            price['spread'] != last['spread']
                        )):
                            changed_prices[symbol] = price
                            last_sent_prices[symbol] = price.copy()
                    static_orders = await get_user_static_orders_cache(redis_client, user_id) or {"open_orders": [], "pending_orders": []}
                    # Get balance and margin from minimal cache (this is what order processing updates)
                    balance_margin_data = await get_user_balance_margin_cache(redis_client, user_id)
                    balance_value = balance_margin_data.get("wallet_balance", "0.0") if balance_margin_data else "0.0"
                    margin_value = balance_margin_data.get("margin", "0.0") if balance_margin_data else "0.0"
                    logger.info(f"[WEBSOCKET_DEBUG] User {user_id}: Reading from balance_margin_cache - balance={balance_value}, margin={margin_value}")
                    logger.info(f"[WEBSOCKET_DEBUG] User {user_id}: Full balance_margin_data from cache: {balance_margin_data}")
                    response_data = {
                        "type": "market_update",
                        "data": {
                            "market_prices": adjusted_prices if is_initial_connection else changed_prices,
                            "account_summary": {
                                "balance": str(balance_value),
                                "margin": str(margin_value),
                                "open_orders": static_orders.get("open_orders", []),
                                "pending_orders": static_orders.get("pending_orders", [])
                            }
                        }
                    }
                    await safe_websocket_send(
                        websocket,
                        json.dumps(response_data, cls=DecimalEncoder),
                        user_id,
                        "market update"
                    )
                    if is_initial_connection:
                        is_initial_connection = False
                elif channel == REDIS_ORDER_UPDATES_CHANNEL and message_data.get("type") == "ORDER_UPDATE" and str(message_data.get("user_id")) == str(user_id):
                    logger.info(f"[WEBSOCKET_DEBUG] User {user_id}: Received ORDER_UPDATE message")
                    static_orders = await get_user_static_orders_cache(redis_client, user_id) or {"open_orders": [], "pending_orders": []}
                    # Get balance and margin from minimal cache (this is what order processing updates)
                    balance_margin_data = await get_user_balance_margin_cache(redis_client, user_id)
                    balance_value = balance_margin_data.get("wallet_balance", "0.0") if balance_margin_data else "0.0"
                    margin_value = balance_margin_data.get("margin", "0.0") if balance_margin_data else "0.0"
                    logger.info(f"[WEBSOCKET_DEBUG] User {user_id}: Reading from balance_margin_cache - balance={balance_value}, margin={margin_value}")
                    logger.info(f"[WEBSOCKET_DEBUG] User {user_id}: Full balance_margin_data from cache: {balance_margin_data}")
                    response_data = {
                        "type": "market_update",
                        "data": {
                            "market_prices": adjusted_prices,
                            "account_summary": {
                                "balance": str(balance_value),
                                "margin": str(margin_value),
                                "open_orders": static_orders.get("open_orders", []),
                                "pending_orders": static_orders.get("pending_orders", [])
                            }
                        }
                    }
                    await safe_websocket_send(
                        websocket,
                        json.dumps(response_data, cls=DecimalEncoder),
                        user_id,
                        "order update"
                    )
                elif channel == REDIS_USER_DATA_UPDATES_CHANNEL and message_data.get("type") == "USER_DATA_UPDATE" and str(message_data.get("user_id")) == str(user_id):
                    logger.info(f"[WEBSOCKET_DEBUG] User {user_id}: Received USER_DATA_UPDATE message")
                    static_orders = await get_user_static_orders_cache(redis_client, user_id) or {"open_orders": [], "pending_orders": []}
                    # Get balance and margin from minimal cache (this is what order processing updates)
                    balance_margin_data = await get_user_balance_margin_cache(redis_client, user_id)
                    balance_value = balance_margin_data.get("wallet_balance", "0.0") if balance_margin_data else "0.0"
                    margin_value = balance_margin_data.get("margin", "0.0") if balance_margin_data else "0.0"
                    logger.info(f"[WEBSOCKET_DEBUG] User {user_id}: Reading from balance_margin_cache - balance={balance_value}, margin={margin_value}")
                    logger.info(f"[WEBSOCKET_DEBUG] User {user_id}: Full balance_margin_data from cache: {balance_margin_data}")
                    response_data = {
                        "type": "market_update",
                        "data": {
                            "market_prices": adjusted_prices,
                            "account_summary": {
                                "balance": str(balance_value),
                                "margin": str(margin_value),
                                "open_orders": static_orders.get("open_orders", []),
                                "pending_orders": static_orders.get("pending_orders", [])
                            }
                        }
                    }
                    await safe_websocket_send(
                        websocket,
                        json.dumps(response_data, cls=DecimalEncoder),
                        user_id,
                        "user data update"
                    )
            except Exception:
                logger.error(f"User {user_id}: Error in message processing", exc_info=True)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.error(f"User {user_id}: Unexpected error in websocket listener", exc_info=True)
    finally:
        await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
        await pubsub.unsubscribe(REDIS_ORDER_UPDATES_CHANNEL)
        await pubsub.unsubscribe(REDIS_USER_DATA_UPDATES_CHANNEL)
        await pubsub.close()

async def update_static_orders_cache(user_id: int, db: AsyncSession, redis_client: Redis, user_type: str):
    """
    Update the static orders cache for a user (open and pending orders without PnL).
    This is called when initializing the WebSocket connection and when order status changes.
    Always fetches fresh data from the database to ensure the cache is up-to-date.
    """
    try:
        order_model = get_order_model(user_type)
        
        # Get open orders - always fetch from database to ensure fresh data
        open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
        logger.info(f"User {user_id}: Fetched {len(open_orders_orm)} open orders directly from database")
        open_orders_data = []
        for pos in open_orders_orm:
            pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                        for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                    'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'order_user_id', 'order_status']}
            pos_dict['commission'] = str(getattr(pos, 'commission', '0.0'))
            # Add created_at field instead of updated_at
            created_at = getattr(pos, 'created_at', None)
            if created_at:
                pos_dict['created_at'] = created_at.isoformat() if isinstance(created_at, datetime.datetime) else str(created_at)
            open_orders_data.append(pos_dict)
        
        # Get pending orders - always fetch from database to ensure fresh data
        pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
        pending_orders_orm = await crud_order.get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model)
        logger.info(f"User {user_id}: Fetched {len(pending_orders_orm)} pending orders directly from database")
        pending_orders_data = []
        for po in pending_orders_orm:
            po_dict = {attr: str(v) if isinstance(v := getattr(po, attr, None), Decimal) else v
                      for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                  'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'order_user_id', 'order_status']}
            po_dict['commission'] = str(getattr(po, 'commission', '0.0'))
            # Add created_at field instead of updated_at
            created_at = getattr(po, 'created_at', None)
            if created_at:
                po_dict['created_at'] = created_at.isoformat() if isinstance(created_at, datetime.datetime) else str(created_at)
            pending_orders_data.append(po_dict)
        
        # Cache the static orders data
        static_orders_data = {
            "open_orders": open_orders_data,
            "pending_orders": pending_orders_data,
            "updated_at": datetime.datetime.now().isoformat()
        }
        await set_user_static_orders_cache(redis_client, user_id, static_orders_data)
        logger.info(f"User {user_id}: Updated static orders cache with {len(open_orders_data)} open orders and {len(pending_orders_data)} pending orders")
        
        return static_orders_data
    except Exception as e:
        logger.error(f"Error updating static orders cache for user {user_id}: {e}", exc_info=True)
        return {"open_orders": [], "pending_orders": [], "updated_at": datetime.datetime.now().isoformat()}
    finally:
        # Ensure database session is properly handled
        try:
            await db.close()
        except Exception:
            pass  # Ignore close errors

async def update_dynamic_portfolio_cache(
    user_id: int,
    group_name: str,
    open_positions: List[Dict[str, Any]],
    adjusted_market_prices: Dict[str, Dict[str, float]],
    redis_client: Redis,
    db: AsyncSession,
    user_type: str
):
    """
    Update the dynamic portfolio cache for a user (free margin, positions with PnL, margin level).
    This is called whenever market data changes.
    """
    try:
        # Get user data for balance, leverage, etc.
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            logger.warning(f"User data not found for user {user_id}. Cannot update dynamic portfolio.")
            return
        
        # Get group symbol settings for contract sizes, etc.
        group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
        if not group_symbol_settings:
            logger.warning(f"Group symbol settings not found for group {group_name}. Cannot update dynamic portfolio.")
            return
        
        # Calculate portfolio metrics using the portfolio calculator
        portfolio_metrics = await calculate_user_portfolio(
            user_data=user_data,
            open_positions=open_positions,
            adjusted_market_prices=adjusted_market_prices,
            group_symbol_settings=group_symbol_settings,
            redis_client=redis_client
        )
        
        # Cache the dynamic portfolio data
        dynamic_portfolio_data = {
            "balance": portfolio_metrics.get("balance", "0.0"),
            "equity": portfolio_metrics.get("equity", "0.0"),
            "margin": portfolio_metrics.get("margin", "0.0"),
            "free_margin": portfolio_metrics.get("free_margin", "0.0"),
            "profit_loss": portfolio_metrics.get("profit_loss", "0.0"),
            "margin_level": portfolio_metrics.get("margin_level", "0.0"),
            "positions_with_pnl": portfolio_metrics.get("positions", [])  # Positions with PnL calculations
        }
        await set_user_dynamic_portfolio_cache(redis_client, user_id, dynamic_portfolio_data)
        logger.debug(f"Updated dynamic portfolio cache for user {user_id}")
        
        return dynamic_portfolio_data
    except Exception as e:
        logger.error(f"Error updating dynamic portfolio cache for user {user_id}: {e}", exc_info=True)
        return None
    finally:
        # Ensure database session is properly handled
        try:
            await db.close()
        except Exception:
            pass  # Ignore close errors

async def process_portfolio_update(
    user_id: int,
    group_name: str,
    redis_client: Redis,
    db: AsyncSession,
    user_type: str,
    adjusted_prices: Dict[str, Dict[str, float]],
    websocket: WebSocket,
    is_initial_connection: bool,
    all_symbols_cache: Dict[str, Dict[str, float]]
):
    """
    Process market data updates, update dynamic portfolio data, and send updates to the client.
    Optimized to use cache instead of database queries on every tick.
    """
    try:
        # Try to get static orders from cache first
        static_orders = await get_user_static_orders_cache(redis_client, user_id)
        
        # Only query database if cache is empty or this is initial connection
        if not static_orders or is_initial_connection:
            if not static_orders:
                logger.info(f"User {user_id}: Static orders cache empty. Fetching from database.")
            else:
                logger.info(f"User {user_id}: Initial connection - fetching fresh orders from database.")
            
            # Create a new database session for fresh data
            async with AsyncSessionLocal() as refresh_db:
                try:
                    static_orders = await update_static_orders_cache(user_id, refresh_db, redis_client, user_type)
                except Exception as e:
                    logger.error(f"User {user_id}: Error updating static orders cache: {e}", exc_info=True)
                    static_orders = {"open_orders": [], "pending_orders": []}
        
        open_positions = static_orders.get("open_orders", []) if static_orders else []
        pending_orders = static_orders.get("pending_orders", []) if static_orders else []
        
        # Update dynamic portfolio cache with current market prices
        try:
            await update_dynamic_portfolio_cache(
                user_id=user_id,
                group_name=group_name,
                open_positions=open_positions,
                adjusted_market_prices=adjusted_prices,
                redis_client=redis_client,
                db=db,
                user_type=user_type
            )
        except Exception as e:
            logger.error(f"User {user_id}: Error updating dynamic portfolio cache: {e}", exc_info=True)
        
        # Get dynamic portfolio from cache
        dynamic_portfolio = await get_user_dynamic_portfolio_cache(redis_client, user_id)
        
        # Get user data from cache (only query DB if cache is empty)
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            logger.warning(f"User {user_id}: User data cache empty. Fetching from database.")
            async with AsyncSessionLocal() as refresh_db:
                try:
                    user_data = await get_user_data_cache(redis_client, user_id, refresh_db, user_type)
                except Exception as e:
                    logger.error(f"User {user_id}: Error fetching user data: {e}", exc_info=True)
        
        logger.info(f"[WEBSOCKET_DEBUG] User {user_id}: user_data margin={user_data.get('margin', 'N/A') if user_data else 'N/A'}")
        
        if not dynamic_portfolio:
            dynamic_portfolio = {
                "balance": user_data.get("wallet_balance", "0.0") if user_data else "0.0",
                "margin": user_data.get("margin", "0.0") if user_data else "0.0",
                "free_margin": "0.0",
                "profit_loss": "0.0",
                "margin_level": "0.0"
            }
        
        # Prepare response data
        balance_value = user_data.get("wallet_balance", "0.0") if user_data else dynamic_portfolio.get("balance", "0.0")
        margin_value = user_data.get("margin", "0.0") if user_data else dynamic_portfolio.get("margin", "0.0")
        
        logger.info(f"[WEBSOCKET_DEBUG] User {user_id}: Final margin_value={margin_value}")
        
        if isinstance(balance_value, Decimal):
            balance_value = str(balance_value)
        if isinstance(margin_value, Decimal):
            margin_value = str(margin_value)
        
        logger.debug(f"User {user_id}: Using balance_value={balance_value}, margin_value={margin_value} for WebSocket response")
        
        # Only send changed/updated symbols after initial connection
        market_prices_to_send = adjusted_prices
        logger.debug(f"User {user_id}: Sending {len(market_prices_to_send)} changed/updated symbols")
        
        response_data = {
            "type": "market_update",
            "data": {
                "market_prices": market_prices_to_send,
                "account_summary": {
                    "balance": balance_value,
                    "margin": margin_value,
                    "open_orders": open_positions,
                    "pending_orders": pending_orders
                }
            }
        }
        
        # Safely send the message
        success = await safe_websocket_send(
            websocket, 
            json.dumps(response_data, cls=DecimalEncoder), 
            user_id, 
            "portfolio update"
        )
        
        if not success:
            logger.info(f"User {user_id}: Failed to send portfolio update, connection may be closed")
            return  # Exit the function since connection is closed
    except Exception as e:
        logger.error(f"User {user_id}: Error processing portfolio update: {e}", exc_info=True)
    finally:
        # Ensure database session is properly handled
        try:
            await db.close()
        except Exception:
            pass  # Ignore close errors


# app/api/v1/endpoints/market_data_ws.py
# ... other imports ...
logger = websocket_logger # or temporarily: import logging; logger = logging.getLogger(__name__)
print("DEBUG: market_data_ws.py module imported!")



from fastapi import WebSocket, Depends
from sqlalchemy.orm import Session
from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client

@router.websocket("/ws/market-data")
async def websocket_endpoint(websocket: WebSocket, db: Session = Depends(get_db)):
    """
    WebSocket endpoint for market data.
    - Authenticates user as before.
    - Accepts the connection as early as possible, then does heavy DB/Redis work.
    - Sends a loading message immediately after accepting.
    """
    connection_start_time = time.time()
    connection_id = f"{websocket.client.host}:{websocket.client.port}-{int(connection_start_time * 1000)}"
    
    logger.info(f"--- MINIMAL TEST: ENTERED websocket_endpoint --- Connection ID: {connection_id}")
    for handler in logger.handlers:
        handler.flush()
    db_user_instance: Optional[User | DemoUser] = None

    # Extract token from query params
    token = websocket.query_params.get("token")
    if token is None:
        logger.warning(f"WebSocket connection attempt without token from {websocket.client.host}:{websocket.client.port}")
        update_connection_metrics(connection_id, 0, False)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Missing token")
        return

    # Clean up token - remove any trailing quotes or encoded characters
    token = token.strip('"').strip("'").replace('%22', '').replace('%27', '')

    # Accept the WebSocket connection as early as possible
    try:
        await websocket.accept()
        connection_time = time.time() - connection_start_time
        update_connection_metrics(connection_id, connection_time, True)
        logger.info(f"WebSocket connection accepted (early) for {websocket.client.host}:{websocket.client.port} in {connection_time:.3f}s")
        
        # Send a loading message to the client
        await safe_websocket_send(
            websocket,
            json.dumps({
                "type": "loading",
                "message": "Initializing connection, please wait..."
            }),
            0,  # user_id not available yet, use 0
            "loading message"
        )
    except Exception as accept_error:
        logger.error(f"Failed to accept WebSocket connection: {accept_error}")
        update_connection_metrics(connection_id, time.time() - connection_start_time, False)
        return

    # Initialize Redis client
    redis_client = await get_redis_client()

    try:
        from jose import JWTError, ExpiredSignatureError
        try:
            payload = decode_token(token)
            account_number = payload.get("account_number")
            user_type = payload.get("user_type", "live")
            if not account_number:
                logger.warning(f"WebSocket auth failed: Invalid token payload - missing account_number")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token: Account number missing")
                return

            # Strictly fetch from correct table based on user_type and account_number
            logger.info(f"WebSocket auth: token payload={payload}, account_number={account_number}, user_type={user_type}")
            if user_type == "demo":
                logger.info(f"WebSocket auth: About to call get_demo_user_by_account_number with account_number={account_number}, user_type={user_type}")
                db_user_instance = await get_demo_user_by_account_number(db, account_number, user_type)
                logger.info(f"WebSocket auth: get_demo_user_by_account_number({account_number}, {user_type}) returned: {db_user_instance}")
            else:
                logger.info(f"WebSocket auth: About to call get_user_by_account_number with account_number={account_number}, user_type={user_type}")
                db_user_instance = await get_user_by_account_number(db, account_number, user_type)
                logger.info(f"WebSocket auth: get_user_by_account_number({account_number}, {user_type}) returned: {db_user_instance}")

            if not db_user_instance:
                logger.warning(f"Authentication failed for account_number {account_number} (type {user_type}): User not found in correct table.")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User not found")
                return
            
            if not getattr(db_user_instance, 'isActive', True):
                logger.warning(f"Authentication failed for user ID {getattr(db_user_instance, 'id', None)}: User inactive.")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User inactive")
                return

        except ExpiredSignatureError:
            logger.warning(f"WebSocket auth failed: Token expired for {websocket.client.host}:{websocket.client.port}")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token expired")
            return
        except JWTError as jwt_err:
            logger.warning(f"WebSocket auth failed: JWT error for {websocket.client.host}:{websocket.client.port}: {str(jwt_err)}")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token")
            return

        group_name = getattr(db_user_instance, 'group_name', 'default')
        
        # Get user ID from the user instance
        db_user_id = getattr(db_user_instance, 'id', None)
        if not db_user_id:
            logger.warning(f"Authentication failed: User instance missing ID field. Account: {account_number}")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid user data")
            return
            
        # Initial caching of user data, portfolio, and group-symbol settings
        user_data_to_cache = {
            "id": getattr(db_user_instance, 'id', None),
            "email": getattr(db_user_instance, 'email', None),
            "account_number": account_number,
            "group_name": group_name,
            "leverage": Decimal(str(getattr(db_user_instance, 'leverage', 1.0))),
            "wallet_balance": Decimal(str(getattr(db_user_instance, 'wallet_balance', 0.0))),
            "margin": Decimal(str(getattr(db_user_instance, 'margin', 0.0))),
            "user_type": user_type,
            "first_name": getattr(db_user_instance, 'first_name', None),
            "last_name": getattr(db_user_instance, 'last_name', None),
            "country": getattr(db_user_instance, 'country', None),
            "phone_number": getattr(db_user_instance, 'phone_number', None)
        }
        await set_user_data_cache(redis_client, db_user_id, user_data_to_cache, user_type)

        # Always use user_type to select the correct order model
        order_model_class = get_order_model(user_type)
        logger.info(f"[WS] Using order model: {order_model_class.__name__} for user_type={user_type}, account_number={account_number}")
        # Use DB user_id (int) for querying open orders
        open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, db_user_id, order_model_class)
        logger.info(f"[WS] Open positions from DB for user_id={db_user_id}: {open_positions_orm}")

        initial_positions_data = []
        for pos in open_positions_orm:
            pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                        for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'order_user_id', 'order_status']}
            pos_dict['commission'] = str(getattr(pos, 'commission', '0.0'))
            # Add created_at field instead of updated_at
            created_at = getattr(pos, 'created_at', None)
            if created_at:
                pos_dict['created_at'] = created_at.isoformat() if isinstance(created_at, datetime.datetime) else str(created_at)
            initial_positions_data.append(pos_dict)

        # Dynamically calculate margin from open positions
        total_margin = sum(Decimal(pos['margin']) for pos in initial_positions_data if 'margin' in pos)
        
        # Set minimal balance/margin cache for websocket
        await set_user_balance_margin_cache(redis_client, db_user_id, user_data_to_cache["wallet_balance"], total_margin)
        
        # Keep the old portfolio cache for backward compatibility (can be removed later)
        user_portfolio_data = {
            "balance": str(user_data_to_cache["wallet_balance"]),
            "equity": "0.0",
            "margin": str(total_margin),
            "free_margin": "0.0",
            "profit_loss": "0.0",
            "margin_level": "0.0",
            "positions": initial_positions_data
        }
        await set_user_portfolio_cache(redis_client, db_user_id, user_portfolio_data)
        await update_group_symbol_settings(group_name, db, redis_client)

    except Exception as e:
        logger.error(f"Unexpected WS auth error for {websocket.client.host}:{websocket.client.port}: {e}", exc_info=True)
        if websocket.client_state != WebSocketState.DISCONNECTED:
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Authentication error")
        return

    # Initialize static orders cache
    static_orders = await update_static_orders_cache(db_user_id, db, redis_client, user_type)
    
    # Initialize dynamic portfolio cache with empty data
    # This will be updated with the first market data update
    initial_dynamic_portfolio = {
        "balance": str(user_data_to_cache["wallet_balance"]),
        "equity": str(user_data_to_cache["wallet_balance"]),
        "margin": str(user_data_to_cache["margin"]),
        "free_margin": str(user_data_to_cache["wallet_balance"]),
        "profit_loss": "0.0",
        "margin_level": "0.0",
        "positions_with_pnl": []
    }
    await set_user_dynamic_portfolio_cache(redis_client, db_user_id, initial_dynamic_portfolio)

    # Send initial connection data with all available symbols
    try:
        # Check if the connection is still alive before proceeding
        if websocket.client_state == WebSocketState.DISCONNECTED:
            logger.warning(f"User {account_number}: Client disconnected before sending initial data")
            return

        # Get all available symbols for the group
        group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
        initial_symbols_data = {}
        
        if group_settings:
            # Get all symbols for this group
            group_symbols = list(group_settings.keys())
            logger.info(f"User {account_number}: Fetching initial market data for {len(group_symbols)} symbols from cache (adjusted or last known price)")
            for symbol in group_symbols:
                # 1. Try adjusted price cache
                cached_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
                if cached_prices:
                    initial_symbols_data[symbol] = {
                        'buy': float(cached_prices.get('buy', 0)),
                        'sell': float(cached_prices.get('sell', 0)),
                        'spread': float(cached_prices.get('spread', 0))
                    }
                    continue
                # 2. Fallback to last known price
                last_price = await get_last_known_price(redis_client, symbol)
                symbol_group_settings = group_settings.get(symbol)
                if last_price and last_price.get('b') and last_price.get('o') and symbol_group_settings:
                    try:
                        raw_ask_price = last_price.get('b')  # Ask
                        raw_bid_price = last_price.get('o')  # Bid
                        ask_decimal = Decimal(str(raw_ask_price))
                        bid_decimal = Decimal(str(raw_bid_price))
                        spread_setting = Decimal(str(symbol_group_settings.get('spread', 0)))
                        spread_pip_setting = Decimal(str(symbol_group_settings.get('spread_pip', 0)))
                        configured_spread_amount = spread_setting * spread_pip_setting
                        half_spread = configured_spread_amount / Decimal(2)
                        adjusted_buy_price = ask_decimal + half_spread
                        adjusted_sell_price = bid_decimal - half_spread
                        effective_spread_price_units = adjusted_buy_price - adjusted_sell_price
                        effective_spread_in_pips = Decimal("0.0")
                        if spread_pip_setting > Decimal("0.0"):
                            effective_spread_in_pips = effective_spread_price_units / spread_pip_setting
                        initial_symbols_data[symbol] = {
                            'buy': float(adjusted_buy_price),
                            'sell': float(adjusted_sell_price),
                            'spread': float(effective_spread_in_pips)
                        }
                        logger.debug(f"User {account_number}: Fallback last price for {symbol}: Buy={adjusted_buy_price}, Sell={adjusted_sell_price}")
                    except Exception as calc_error:
                        logger.error(f"User {account_number}: Error calculating adjusted prices for {symbol} from last known price: {calc_error}")
                    continue
                # 3. If neither, skip or send placeholder (optional)
                # initial_symbols_data[symbol] = {'buy': 0.0, 'sell': 0.0, 'spread': 0.0}
        
        # Check connection state again before sending
        if websocket.client_state == WebSocketState.CONNECTED:
            # Send initial connection message with all symbols data
            initial_response = {
                "type": "market_update",
                "data": {
                    "market_prices": initial_symbols_data,
                    "account_summary": {
                        "balance": str(user_data_to_cache["wallet_balance"]),
                        "margin": str(total_margin),  # Use calculated total_margin instead of user_data_to_cache["margin"]
                        "open_orders": static_orders.get("open_orders", []) if static_orders else [],
                        "pending_orders": static_orders.get("pending_orders", []) if static_orders else []
                    }
                }
            }
            
            # Safely send initial connection data
            success = await safe_websocket_send(
                websocket,
                json.dumps(initial_response, cls=DecimalEncoder),
                db_user_id,
                "initial connection data"
            )
            
            if success:
                logger.info(f"User {account_number}: Sent initial connection data with {len(initial_symbols_data)} symbols (fresh from cache)")
            else:
                logger.warning(f"User {account_number}: Failed to send initial connection data, connection may be closed")
                return
        else:
            logger.warning(f"User {account_number}: Client disconnected before sending initial data")
            return

    except Exception as e:
        logger.error(f"User {account_number}: Error sending initial connection data: {e}", exc_info=True)

    # Create and manage the per-connection Redis listener task
    listener_task = asyncio.create_task(
        per_connection_redis_listener(websocket, db_user_id, group_name, redis_client, db, user_type)
    )

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        logger.info(f"User {account_number}: WebSocket disconnected by client.")
    except Exception as e:
        logger.error(f"User {account_number}: Error in main WebSocket loop: {e}", exc_info=True)
    finally:
        logger.info(f"User {account_number}: Cleaning up WebSocket connection.")
        remove_connection_metrics()
        
        if not listener_task.done():
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                logger.info(f"User {account_number}: Listener task successfully cancelled.")
            except Exception as task_e:
                logger.error(f"User {account_number}: Error during listener task cleanup: {task_e}", exc_info=True)
        
        if websocket.client_state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
            except Exception as close_e:
                logger.error(f"User {account_number}: Error explicitly closing WebSocket: {close_e}", exc_info=True)
        logger.info(f"User {account_number}: WebSocket connection fully closed.")


# --- Helper Function to Update Group Symbol Settings (used by websocket_endpoint) ---
# This function remains largely the same.
async def update_group_symbol_settings(group_name: str, db: AsyncSession, redis_client: Redis):
    if not group_name:
        logger.warning("Cannot update group-symbol settings: group_name is missing.")
        return
    try:
        group_settings_list = await crud_group.get_groups(db, search=group_name)
        if not group_settings_list:
             logger.warning(f"No group settings found in DB for group '{group_name}'.")
             return
        for group_setting in group_settings_list:
            symbol_name = getattr(group_setting, 'symbol', None)
            if symbol_name:
                settings = {
                    # ... (all your settings fields) ...
                    "commision_type": getattr(group_setting, 'commision_type', None),"commision_value_type": getattr(group_setting, 'commision_value_type', None),"type": getattr(group_setting, 'type', None),"pip_currency": getattr(group_setting, 'pip_currency', "USD"),"show_points": getattr(group_setting, 'show_points', None),"swap_buy": getattr(group_setting, 'swap_buy', decimal.Decimal(0.0)),"swap_sell": getattr(group_setting, 'swap_sell', decimal.Decimal(0.0)),"commision": getattr(group_setting, 'commision', decimal.Decimal(0.0)),"margin": getattr(group_setting, 'margin', decimal.Decimal(0.0)),"spread": getattr(group_setting, 'spread', decimal.Decimal(0.0)),"deviation": getattr(group_setting, 'deviation', decimal.Decimal(0.0)),"min_lot": getattr(group_setting, 'min_lot', decimal.Decimal(0.0)),"max_lot": getattr(group_setting, 'max_lot', decimal.Decimal(0.0)),"pips": getattr(group_setting, 'pips', decimal.Decimal(0.0)),"spread_pip": getattr(group_setting, 'spread_pip', decimal.Decimal(0.0)),"contract_size": getattr(group_setting, 'contract_size', decimal.Decimal("100000")),
                }
                # Fetch profit_currency from Symbol model
                symbol_obj_stmt = select(Symbol).filter_by(name=symbol_name.upper())
                symbol_obj_result = await db.execute(symbol_obj_stmt)
                symbol_obj = symbol_obj_result.scalars().first()
                if symbol_obj and symbol_obj.profit_currency:
                    settings["profit_currency"] = symbol_obj.profit_currency
                else: # Fallback
                    settings["profit_currency"] = getattr(group_setting, 'pip_currency', 'USD')
                # Fetch contract_size from ExternalSymbolInfo (overrides group if found)
                external_symbol_obj_stmt = select(ExternalSymbolInfo).filter_by(fix_symbol=symbol_name) # Case-sensitive match?
                external_symbol_obj_result = await db.execute(external_symbol_obj_stmt)
                external_symbol_obj = external_symbol_obj_result.scalars().first()
                if external_symbol_obj and external_symbol_obj.contract_size is not None:
                    settings["contract_size"] = external_symbol_obj.contract_size
                
                await set_group_symbol_settings_cache(redis_client, group_name, symbol_name.upper(), settings)
            else:
                 logger.warning(f"Group setting symbol is None for group '{group_name}'.")
        logger.debug(f"Cached/updated group-symbol settings for group '{group_name}'.")
    except Exception as e:
        logger.error(f"Error caching group-symbol settings for '{group_name}': {e}", exc_info=True)


# --- Redis Publisher Task (Publishes from Firebase queue to general market data channel) ---
# This function remains the same.
async def redis_publisher_task(redis_client: Redis):
    logger.info("Redis publisher task started. Publishing to channel '%s'.", REDIS_MARKET_DATA_CHANNEL)
    if not redis_client:
        logger.critical("Redis client not provided for publisher task. Exiting.")
        return
    try:
        while True:
            raw_market_data_message = await redis_publish_queue.get()
            if raw_market_data_message is None: # Shutdown signal
                logger.info("Publisher task received shutdown signal. Exiting.")
                break
            try:
                # DIAGNOSTICS: Keep the _timestamp key to measure delays.
                message_to_publish_data = raw_market_data_message.copy()
                
                # Check if there is meaningful data besides the timestamp
                if any(k != '_timestamp' for k in message_to_publish_data.keys()):
                     message_to_publish_data["type"] = "market_data_update" # Standardize type for raw updates
                     message_to_publish = json.dumps(message_to_publish_data, cls=DecimalEncoder)
                else: # Skip if only timestamp was present
                     redis_publish_queue.task_done()
                     continue
            except Exception as e:
                logger.error(f"Publisher failed to serialize message: {e}. Skipping.", exc_info=True)
                redis_publish_queue.task_done()
                continue
            try:
                await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message_to_publish)
            except Exception as e:
                logger.error(f"Publisher failed to publish to Redis: {e}. Msg: {message_to_publish[:100]}...", exc_info=True)
            redis_publish_queue.task_done()
    except asyncio.CancelledError:
        logger.info("Redis publisher task cancelled.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: Redis publisher task failed: {e}", exc_info=True)
    finally:
        logger.info("Redis publisher task finished.")

# REMOVE redis_market_data_broadcaster function entirely
# Its functionality is now distributed into per_connection_redis_listener tasks managed by websocket_endpoint

# Add a debug endpoint to manually trigger order updates
@router.post("/debug/publish-order-update/{user_id}")
async def debug_publish_order_update(
    user_id: int,
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Debug endpoint to manually publish an order update message to Redis.
    This is useful for testing WebSocket functionality.
    """
    try:
        message = json.dumps({
            "type": "ORDER_UPDATE",
            "user_id": user_id,
            "timestamp": datetime.datetime.now().isoformat()
        }, cls=DecimalEncoder)
        
        result = await redis_client.publish(REDIS_ORDER_UPDATES_CHANNEL, message)
        logger.info(f"DEBUG: Published order update for user {user_id} to {REDIS_ORDER_UPDATES_CHANNEL}, received by {result} subscribers")
        
        return {"status": "success", "message": f"Order update published for user {user_id}", "subscribers": result}
    except Exception as e:
        logger.error(f"Error in debug_publish_order_update: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error publishing order update: {str(e)}")

# Add a debug endpoint to test the WebSocket order updates
@router.post("/debug/refresh-orders/{user_id}")
async def debug_refresh_orders(
    user_id: int,
    user_type: str = Query("demo"),
    redis_client: Redis = Depends(get_redis_client),
    db: AsyncSession = Depends(get_db)
):
    """
    Debug endpoint to manually refresh the static orders cache from the database.
    This bypasses Redis and ensures fresh data is fetched.
    """
    try:
        # Create a new database session for this operation
        async with AsyncSessionLocal() as refresh_db:
            # Force refresh of static orders cache from database
            static_orders = await update_static_orders_cache(user_id, refresh_db, redis_client, user_type)
            
        # Log the static orders that were fetched
        open_orders_count = len(static_orders.get("open_orders", []))
        pending_orders_count = len(static_orders.get("pending_orders", []))
        
        # Publish an order update message to Redis
        message = json.dumps({
            "type": "ORDER_UPDATE",
            "user_id": user_id,
            "timestamp": datetime.datetime.now().isoformat()
        }, cls=DecimalEncoder)
        
        result = await redis_client.publish(REDIS_ORDER_UPDATES_CHANNEL, message)
        logger.info(f"DEBUG: Published order update for user {user_id} to {REDIS_ORDER_UPDATES_CHANNEL}, received by {result} subscribers")
        
        return {
            "status": "success", 
            "message": f"Orders refreshed for user {user_id}", 
            "open_orders": open_orders_count,
            "pending_orders": pending_orders_count,
            "subscribers": result
        }
    except Exception as e:
        logger.error(f"Error in debug_refresh_orders: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error refreshing orders: {str(e)}")


# Add monitoring endpoints
@router.get("/monitoring/websocket-stats")
async def get_websocket_stats():
    """Get WebSocket connection statistics"""
    with metrics_lock:
        stats = connection_metrics.copy()
        
        # Calculate average connection time
        if stats['connection_times']:
            stats['avg_connection_time'] = sum(stats['connection_times']) / len(stats['connection_times'])
            stats['min_connection_time'] = min(stats['connection_times'])
            stats['max_connection_time'] = max(stats['connection_times'])
        else:
            stats['avg_connection_time'] = 0
            stats['min_connection_time'] = 0
            stats['max_connection_time'] = 0
        
        # Calculate success rate
        total_attempts = stats['total_connections']
        successful_attempts = total_attempts - stats['failed_connections']
        stats['success_rate'] = (successful_attempts / total_attempts * 100) if total_attempts > 0 else 0
        
        # Add timestamp
        stats['timestamp'] = time.time()
        
        return stats

@router.get("/monitoring/active-connections")
async def get_active_connections():
    """Get currently active WebSocket connections"""
    with metrics_lock:
        return {
            'current_connections': connection_metrics['current_connections'],
            'max_concurrent_connections': connection_metrics['max_concurrent_connections'],
            'timestamp': time.time()
        }

@router.post("/monitoring/reset-stats")
async def reset_websocket_stats():
    """Reset WebSocket statistics (for testing)"""
    with metrics_lock:
        global connection_metrics
        connection_metrics = {
            'total_connections': 0,
            'current_connections': 0,
            'max_concurrent_connections': 0,
            'connection_times': [],
            'failed_connections': 0,
            'last_connection_time': None
        }
    return {"message": "WebSocket statistics reset"}