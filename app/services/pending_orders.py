# app/services/pending_orders.py

from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from redis.asyncio import Redis
import logging
from datetime import datetime, timezone
import json
from pydantic import BaseModel 
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select  # Add this import
import asyncio
import time
import uuid
import inspect  # Added for debug function


# Import epsilon configuration
from app.main import SLTP_EPSILON

from app.core.cache import (
    set_user_data_cache, get_user_data_cache,
    set_user_portfolio_cache, get_user_portfolio_cache,
    set_adjusted_market_price_cache, get_adjusted_market_price_cache,
    set_group_symbol_settings_cache, get_group_symbol_settings_cache,
    set_last_known_price, get_last_known_price,
    set_user_static_orders_cache, get_user_static_orders_cache,
    set_user_dynamic_portfolio_cache, get_user_dynamic_portfolio_cache,
    DecimalEncoder, decode_decimal,
    publish_order_update, publish_user_data_update,
    publish_account_structure_changed_event,
    get_group_symbol_settings_cache, 
    get_adjusted_market_price_cache,
    publish_order_update,
    publish_user_data_update,
    publish_market_data_trigger,
    set_user_static_orders_cache,
    get_user_static_orders_cache,
    DecimalEncoder,  # Import for JSON serialization of decimals
    # Balance/margin cache for websocket
    set_user_balance_margin_cache,
)
from app.services.margin_calculator import calculate_single_order_margin
from app.services.portfolio_calculator import calculate_user_portfolio, _convert_to_usd
from app.core.firebase import send_order_to_firebase, get_latest_market_data
from app.database.models import User, DemoUser, UserOrder, DemoUserOrder, ExternalSymbolInfo, Wallet
from app.crud import crud_order
from app.crud.user import update_user_margin, get_user_by_id, get_demo_user_by_id
from app.crud.crud_order import get_open_orders_by_user_id_and_symbol, get_order_model
from app.schemas.order import PendingOrderPlacementRequest, OrderPlacementRequest
from app.schemas.wallet import WalletCreate
# Import ALL necessary functions from order_processing to ensure consistency
from app.services.order_processing import (
    calculate_total_symbol_margin_contribution,  # Use THIS implementation, not the one from margin_calculator
    get_external_symbol_info,
    OrderProcessingError,
    InsufficientFundsError,
    generate_unique_10_digit_id
)
from app.core.logging_config import orders_logger, redis_logger

logger = orders_logger

# Redis key prefix for pending orders
REDIS_PENDING_ORDERS_PREFIX = "pending_orders"



async def remove_pending_order(redis_client: Redis, order_id: str, symbol: str, order_type: str, user_id: str):
    """
    Remove a pending order from Redis.
    """
    try:
        # Remove from the specific pending orders list
        pending_key = f"pending_orders:{symbol}:{order_type}:{user_id}"
        await redis_client.lrem(pending_key, 0, order_id)
        
        # Also remove from the general pending orders list
        general_pending_key = f"pending_orders:{user_id}"
        await redis_client.lrem(general_pending_key, 0, order_id)
        
    except Exception as e:
        logger.error(f"[REDIS_CLEANUP] Error removing pending order {order_id} from Redis: {e}")

async def get_all_pending_orders_from_redis(redis_client: Redis) -> List[Dict[str, Any]]:
    """
    Get all pending orders from Redis for cleanup purposes.
    Returns a list of order data dictionaries.
    Handles the HASH data structure where pending orders are stored.
    """
    try:
        all_pending_orders = []
        
        # Pattern for keys that store pending orders as Hashes.
        pattern = f"{REDIS_PENDING_ORDERS_PREFIX}:*:*"
        keys = await redis_client.keys(pattern)
        
        for key in keys:
            try:
                # This key is a HASH where each field is a user_id and the value is a JSON string of their orders.
                user_orders_map = await redis_client.hgetall(key)
                
                for user_id, orders_json in user_orders_map.items():
                    if orders_json:
                        try:
                            # Decode and parse the list of orders for the user.
                            orders = json.loads(orders_json, object_hook=decode_decimal)
                            all_pending_orders.extend(orders)
                        except json.JSONDecodeError:
                            logger.error(f"[REDIS_CLEANUP] Failed to decode JSON for key {key}, user {user_id}: {orders_json}")
                            continue
            except Exception as e:
                # This handles cases where a key matching the pattern is not a HASH.
                logger.error(f"[REDIS_CLEANUP] Error processing Redis key {key}: {e}")
                continue
        
        return all_pending_orders
        
    except Exception as e:
        redis_logger.error(f"[REDIS_CLEANUP] Error getting all pending orders from Redis: {e}")
        return []

async def add_pending_order(
    redis_client: Redis, 
    pending_order_data: Dict[str, Any]
) -> None:
    """Adds a pending order to Redis."""
    symbol = pending_order_data['order_company_name']
    order_type = pending_order_data['order_type']
    user_id = str(pending_order_data['order_user_id'])  # Ensure user_id is a string
    redis_key = f"{REDIS_PENDING_ORDERS_PREFIX}:{symbol}:{order_type}"

    try:
        all_user_orders_json = await redis_client.hget(redis_key, user_id)
        
        # Handle both bytes and string JSON
        if all_user_orders_json:
            if isinstance(all_user_orders_json, bytes):
                all_user_orders_json = all_user_orders_json.decode('utf-8')
            current_orders = json.loads(all_user_orders_json)
        else:
            current_orders = []

        # Check if an order with the same ID already exists
        if any(order.get('order_id') == pending_order_data['order_id'] for order in current_orders):
            logger.warning(f"Pending order {pending_order_data['order_id']} already exists in Redis. Skipping add.")
            return

        current_orders.append(pending_order_data)
        await redis_client.hset(redis_key, user_id, json.dumps(current_orders))
    except Exception as e:
        logger.error(f"Error adding pending order {pending_order_data['order_id']} to Redis: {e}", exc_info=True)
        raise

async def trigger_pending_order(
    db,
    redis_client: Redis,
    order: Dict[str, Any],
    current_price: Decimal
) -> None:
    """
    Trigger a pending order for any user type.
    Updates the order status to 'OPEN' in the database,
    adjusts user margin, and updates portfolio caches.
    """
    order_id = order['order_id']
    user_id = order['order_user_id']
    user_type = order['user_type']
    symbol = order['order_company_name']
    order_type_original = order['order_type'] # Store original order_type
    from app.core.logging_config import orders_logger
    
    try:
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            user_model = User if user_type == 'live' else DemoUser
            user_data = await user_model.by_id(db, user_id)
            if not user_data:
                orders_logger.error(f"[PENDING_ORDER] User data not found for user {user_id} when triggering order {order_id}. Skipping.")
                return

        group_name = user_data.get('group_name')
        group_settings = await get_group_settings_cache(redis_client, group_name)
        if not group_settings:
            orders_logger.error(f"[PENDING_ORDER] Group settings not found for group {group_name} when triggering order {order_id}. Skipping.")
            return

        order_model = get_order_model(user_type)
        
        # Add enhanced retry logic for database order fetch
        max_retries = 5  # Increase from 3 to 5
        initial_retry_delay = 0.5  # Start with a shorter delay (seconds)
        retry_delay = initial_retry_delay
        db_order = None
        
        for retry_count in range(max_retries):
            # Try the standard method first
            db_order = await crud_order.get_order_by_id(db, order_id, order_model)
            
            if db_order:
                break
                
            # On the last attempt, try a direct SQL query as a backup method
            if retry_count == max_retries - 1:
                try:
                    from sqlalchemy import text
                    table_name = "demo_user_orders" if user_type == "demo" else "user_orders"
                    result = await db.execute(text(f"SELECT * FROM {table_name} WHERE order_id = :order_id"), {"order_id": order_id})
                    row = result.fetchone()
                    if row:
                        # Create order object from raw SQL result
                        db_order = order_model()
                        for key, value in row._mapping.items():
                            setattr(db_order, key, value)
                        break
                except Exception as sql_error:
                    orders_logger.error(f"[PENDING_ORDER] Error in direct SQL query: {str(sql_error)}", exc_info=True)
            
            if retry_count < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 1.5  # Less aggressive exponential backoff

        if not db_order:
            orders_logger.error(f"[PENDING_ORDER] Database order {order_id} not found after {max_retries} attempts when triggering pending order. Skipping.")
            # Remove the order from Redis since it cannot be found in the database
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id)
            return

        # Ensure atomicity: only update if still PENDING
        if db_order.order_status != 'PENDING':
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id) 
            return

        # Get the adjusted buy price (ask price) for the symbol from cache
        group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        adjusted_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
        
        if not adjusted_prices:
            orders_logger.error(f"[PENDING_ORDER] Adjusted market prices not found for symbol {symbol} when triggering order {order_id}. Skipping.")
            return
        
        # Use the adjusted buy price for all trigger conditions
        adjusted_buy_price = adjusted_prices.get('buy')
        if not adjusted_buy_price:
            orders_logger.error(f"[PENDING_ORDER] Adjusted buy price missing for symbol {symbol} when checking order {order_id}. Skipping execution.")
            return
        
        adjusted_sell_price = adjusted_prices.get('sell')
        if not adjusted_sell_price:
            orders_logger.error(f"[PENDING_ORDER] Adjusted sell price missing for symbol {symbol} when checking order {order_id}. Skipping execution.")
            return

        # Normalize decimal values for comparison - round to 5 decimal places
        try:
            order_price = Decimal(str(db_order.order_price))
            # Round to 5 decimal places for consistent comparison
            order_price_normalized = Decimal(str(round(order_price, 5)))
            
            adjusted_buy_price_str = str(adjusted_buy_price)
            # Ensure the price has at least 5 decimal places
            if '.' in adjusted_buy_price_str:
                integer_part, decimal_part = adjusted_buy_price_str.split('.')
                if len(decimal_part) < 5:
                    decimal_part = decimal_part.ljust(5, '0')
                adjusted_buy_price_str = f"{integer_part}.{decimal_part}"
            
            adjusted_buy_price_normalized = Decimal(str(round(Decimal(adjusted_buy_price_str), 5)))
            
            # Similar normalization for adjusted_sell_price if needed
            adjusted_sell_price_str = str(adjusted_sell_price)
            if '.' in adjusted_sell_price_str:
                integer_part, decimal_part = adjusted_sell_price_str.split('.')
                if len(decimal_part) < 5:
                    decimal_part = decimal_part.ljust(5, '0')
                adjusted_sell_price_str = f"{integer_part}.{decimal_part}"
            
            adjusted_sell_price_normalized = Decimal(str(round(Decimal(adjusted_sell_price_str), 5)))
            
        except Exception as e:
            orders_logger.error(f"[PENDING_ORDER] Error normalizing prices for comparison: {str(e)}", exc_info=True)
            return
        
        # Only use adjusted_buy_price for all pending order types
        if not adjusted_buy_price:
            orders_logger.error(f"[PENDING_ORDER] Adjusted buy price missing for symbol {symbol} when checking order {order_id}. Skipping execution.")
            return
        
        # Determine if the order should be triggered
        should_trigger = False
        if order_type_original in ['BUY_LIMIT', 'SELL_STOP']:
            should_trigger = adjusted_buy_price_normalized <= order_price_normalized
        elif order_type_original in ['SELL_LIMIT', 'BUY_STOP']:
            should_trigger = adjusted_buy_price_normalized >= order_price_normalized
        else:
            orders_logger.error(f"[PENDING_ORDER] Unknown order type {order_type_original} for order {order_id}. Skipping execution.")
            return
        
        # Compare with small epsilon tolerance to catch very close values
        epsilon = Decimal(SLTP_EPSILON)  # Small tolerance
        price_diff = abs(adjusted_buy_price_normalized - order_price_normalized)
        is_close = price_diff < epsilon
        
        # Consider using epsilon for near-exact matches
        should_trigger_with_epsilon = False
        if order_type_original in ['BUY_LIMIT', 'SELL_STOP']:
            should_trigger_with_epsilon = (adjusted_buy_price_normalized <= order_price_normalized) or is_close
        elif order_type_original in ['SELL_LIMIT', 'BUY_STOP']:
            should_trigger_with_epsilon = (adjusted_buy_price_normalized >= order_price_normalized) or is_close
        
        # Use epsilon-based trigger when prices are very close
        if should_trigger_with_epsilon and not should_trigger:
            should_trigger = True
        
        if not should_trigger:
            return
        
        # Calculate the required margin for the order using calculate_single_order_margin
        order_quantity_decimal = Decimal(str(db_order.order_quantity))
        user_leverage = Decimal(str(user_data.get('leverage', '1.0')))
        # Get external symbol info from DB
        external_symbol_info = await get_external_symbol_info(db, symbol)
        
        # Get raw market data (for margin calculation) using the synchronous version
        from app.firebase_stream import get_latest_market_data as get_latest_market_data_sync
        raw_market_data = get_latest_market_data_sync(symbol)
        if not raw_market_data or not raw_market_data.get('o'):
            # Fallback to last known price from Redis
            last_known = await get_last_known_price(redis_client, symbol)
            if last_known:
                raw_market_data = last_known
            else:
                orders_logger.warning(f"[PENDING_ORDER] No market data or last known price for {symbol}. Cannot calculate margin for order {order_id}.")
                return
        
        try:
            # Wrap the calculate_single_order_margin call in a try block to catch any exceptions
            margin, exec_price, contract_value, commission = await calculate_single_order_margin(
                redis_client=redis_client,
                symbol=symbol,
                order_type=order_type_original,
                quantity=order_quantity_decimal,
                user_leverage=user_leverage,
                group_settings=group_symbol_settings,
                external_symbol_info=external_symbol_info or {},
                raw_market_data={symbol: raw_market_data},
                db=db,
                user_id=user_id
            )
            
            # Implement proper margin calculation using the same approach as order_processing.py
            try:
                # Step 1: Get all open orders for the symbol
                open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
                    db, user_id, symbol, order_model
                )
                
                # Store original calculated margin for the individual order record
                original_order_margin = margin
                
                # Step 2: Calculate total margin before adding the new order
                margin_before_data = await calculate_total_symbol_margin_contribution(
                    db, redis_client, user_id, symbol, open_orders_for_symbol, order_model, user_type
                )
                margin_before = margin_before_data["total_margin"]
                
                # Step 3: Create a simulated order object for the new order
                new_order_type = 'BUY' if order_type_original.startswith('BUY') else 'SELL'
                simulated_order = type('Obj', (object,), {
                    'order_quantity': order_quantity_decimal,
                    'order_type': new_order_type,
                    'margin': original_order_margin,
                    'id': None,  # Add id attribute to match real orders
                    'order_id': 'NEW_PENDING_TRIGGERED'  # Add order_id attribute for logging
                })()
                
                # Step 4: Calculate total margin after adding the new order
                margin_after_data = await calculate_total_symbol_margin_contribution(
                    db, redis_client, user_id, symbol, 
                    open_orders_for_symbol + [simulated_order],
                    order_model, user_type
                )
                margin_after = margin_after_data["total_margin"]
                
                # Step 5: Calculate additional margin required (can be negative if hedging reduces margin)
                # Let the calculate_total_symbol_margin_contribution function handle all the hedging logic
                margin_difference = max(Decimal("0.0"), margin_after - margin_before)
                
                # Use margin_difference for updating user's total margin
                # This can be positive (adding margin) or negative (reducing margin due to hedging)
                margin = margin_difference
                
            except Exception as margin_calc_error:
                orders_logger.error(f"[PENDING_ORDER] Error calculating margin effect for user {user_id}: {str(margin_calc_error)}", exc_info=True)
                # If the advanced calculation fails, fall back to using the original calculated margin
                margin = original_order_margin 
        except Exception as margin_error:
            orders_logger.error(f"[PENDING_ORDER] Error calculating margin for order {order_id}: {str(margin_error)}", exc_info=True)
            return
        
        # Fetch the latest dynamic portfolio to get up-to-date free_margin
        try:
            dynamic_portfolio = await get_user_dynamic_portfolio_cache(redis_client, user_id)
            if dynamic_portfolio:
                user_free_margin = Decimal(str(dynamic_portfolio.get('free_margin', '0.0')))
            else:
                # Fallback: calculate free margin from user data
                current_wallet_balance_decimal = Decimal(str(user_data.get('wallet_balance', '0')))
                current_total_margin_decimal = Decimal(str(user_data.get('margin', '0')))
                user_free_margin = current_wallet_balance_decimal - current_total_margin_decimal
        except Exception as portfolio_error:
            orders_logger.error(f"[PENDING_ORDER] Error fetching dynamic portfolio for user {user_id}: {str(portfolio_error)}", exc_info=True)
            # Fallback: calculate free margin from user data
            current_wallet_balance_decimal = Decimal(str(user_data.get('wallet_balance', '0')))
            current_total_margin_decimal = Decimal(str(user_data.get('margin', '0')))
            user_free_margin = current_wallet_balance_decimal - current_total_margin_decimal
        
        # Check if user has sufficient free margin for the new order
        if margin > user_free_margin:
            orders_logger.warning(f"[PENDING_ORDER] Order {order_id} for user {user_id} canceled due to insufficient free margin. Required: {margin}, Available free margin: {user_free_margin}")
            db_order.order_status = 'CANCELLED'
            db_order.cancel_message = "InsufficientFreeMargin"
            try:
                await db.commit()
                await db.refresh(db_order)
            except Exception as commit_error:
                orders_logger.error(f"[PENDING_ORDER] Error committing cancelled order {order_id}: {str(commit_error)}", exc_info=True)
                await db.rollback()
            
            # Remove from pending orders
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id)
            return
        
        # Get contract size and profit currency from symbol settings
        contract_size = Decimal(str(group_symbol_settings.get('contract_size', 100000)))
        profit_currency = group_symbol_settings.get('profit', 'USD')
        
        # Calculate contract value using the CORRECT formula - this should match what we calculated above
        contract_value = order_quantity_decimal * contract_size

        # Update the order properties with the calculated values
        try:
            # Store the original calculated margin in the order (without any hedging adjustments)
            # This ensures we always record the true margin requirement for this individual order
            db_order.margin = original_order_margin
            db_order.contract_value = contract_value
            db_order.commission = commission
            db_order.order_price = exec_price  # Use the execution price from margin calculation
        except Exception as value_update_error:
            orders_logger.error(f"[PENDING_ORDER] Error updating order with calculated values: {str(value_update_error)}", exc_info=True)

        # Determine the new order type (removing LIMIT/STOP)
        try:
            new_order_type = 'BUY' if order_type_original in ['BUY_LIMIT', 'BUY_STOP'] else 'SELL'
            db_order.open_time = datetime.now(timezone.utc) 
            db_order.order_type = new_order_type
            # Set stop_loss and take_profit to None
            db_order.stop_loss = None
            db_order.take_profit = None
        except Exception as update_error:
            orders_logger.error(f"[PENDING_ORDER] Error updating order object with new values: {str(update_error)}", exc_info=True)
            return

        # Non-Barclays user
        # FIX: Refresh user data right before margin calculation to get current state
        try:
            # Get fresh user data from database right before margin calculation
            user = None
            if user_type == 'live':
                from app.crud.user import get_user_by_id
                user = await get_user_by_id(db, user_id, user_type='live')
                if not user:
                    # Try demo table if live lookup fails
                    from app.crud.user import get_demo_user_by_id
                    user = await get_demo_user_by_id(db, user_id)
            else:
                from app.crud.user import get_demo_user_by_id
                user = await get_demo_user_by_id(db, user_id)
                if not user:
                    # Try live table if demo lookup fails
                    from app.crud.user import get_user_by_id
                    user = await get_user_by_id(db, user_id, user_type='live')

            if not user:
                logger.error(f"[SLTP_CHECK] User {user_id} not found in either live or demo tables for order {get_attr(order, 'order_id')}")
                return
                
            current_wallet_balance_decimal = Decimal(str(user.wallet_balance))
            current_total_margin_decimal = Decimal(str(user.margin))
            
        except Exception as user_refresh_error:
            orders_logger.error(f"[PENDING_ORDER] Error refreshing user data: {str(user_refresh_error)}", exc_info=True)
            # Fallback to original user data
            current_wallet_balance_decimal = Decimal(str(user_data.get('wallet_balance', '0')))
            current_total_margin_decimal = Decimal(str(user_data.get('margin', '0')))

        # FIX: Only update the margin if there's a real change needed
        # For perfect hedging, if margin is zero, we don't need to update the user's overall margin at all
        if margin > Decimal('0'):
            new_total_margin = current_total_margin_decimal + margin
        else:
            # Perfect hedging case - no need to update the user's margin
            new_total_margin = current_total_margin_decimal

        try:
            # Update user's margin with the new total that includes hedging adjustments
            # Only update margin in the database if there's a change to avoid unnecessary DB operations
            if new_total_margin != current_total_margin_decimal:
                await update_user_margin(
                    db,
                    user_id,
                    user_type,
                    new_total_margin
                )
            
            # Always refresh user data in the cache, even for a perfect hedge
            try:
                # Get updated user data from the database
                user_model = User if user_type == 'live' else DemoUser
                updated_user = await db.execute(select(user_model).filter(user_model.id == user_id))
                updated_user = updated_user.scalars().first()
                
                if updated_user:
                    # Update the user data cache with fresh data from database
                    user_data_to_cache = {
                        "id": updated_user.id,
                        "email": getattr(updated_user, 'email', None),
                        "group_name": updated_user.group_name,
                        "leverage": updated_user.leverage,
                        "user_type": user_type,
                        "account_number": getattr(updated_user, 'account_number', None),
                        "wallet_balance": updated_user.wallet_balance,
                        "margin": updated_user.margin,  # This contains the new margin value
                        "first_name": getattr(updated_user, 'first_name', None),
                        "last_name": getattr(updated_user, 'last_name', None),
                        "country": getattr(updated_user, 'country', None),
                        "phone_number": getattr(updated_user, 'phone_number', None),
                    }
                    await set_user_data_cache(redis_client, user_id, user_data_to_cache)
            except Exception as cache_error:
                orders_logger.error(f"[PENDING_ORDER] Error updating user data cache: {str(cache_error)}", exc_info=True)
            
            # Publish user data update notification to WebSocket clients using the existing cache function
            await publish_user_data_update(redis_client, user_id)
        except Exception as margin_update_error:
            orders_logger.error(f"[PENDING_ORDER] Error updating user margin: {str(margin_update_error)}", exc_info=True)
            return
        
        db_order.order_status = 'OPEN'

        # Commit DB changes for the order status and updated fields
        try:
            await db.commit()
            await db.refresh(db_order)
        except Exception as commit_error:
            orders_logger.error(f"[PENDING_ORDER] Error committing order status change to database: {str(commit_error)}", exc_info=True)
            return

        try:
            # --- Portfolio Update & Websocket Event ---
            user_data_for_portfolio = await get_user_data_cache(redis_client, user_id, db, user_type) # Re-fetch updated user data
            if user_data_for_portfolio:
                open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                open_positions_dicts = []
                
                # Convert order objects to dictionaries safely
                for o in open_orders:
                    if hasattr(o, 'to_dict'):
                        open_positions_dicts.append(o.to_dict())
                    else:
                        # Create a dictionary manually if to_dict method is not available
                        order_dict = {
                            'order_id': getattr(o, 'order_id', None),
                            'order_company_name': getattr(o, 'order_company_name', None),
                            'order_type': getattr(o, 'order_type', None),
                            'order_quantity': getattr(o, 'order_quantity', None),
                            'order_price': getattr(o, 'order_price', None),
                            'margin': getattr(o, 'margin', None),
                            'contract_value': getattr(o, 'contract_value', None),
                            'stop_loss': getattr(o, 'stop_loss', None),
                            'take_profit': getattr(o, 'take_profit', None),
                            'commission': getattr(o, 'commission', '0.0'),
                            'order_status': getattr(o, 'order_status', None),
                            'order_user_id': getattr(o, 'order_user_id', None)
                        }
                        open_positions_dicts.append(order_dict)
                
                # Fetch current prices for all open positions to calculate portfolio correctly
                adjusted_market_prices = {}
                if group_symbol_settings: # Ensure group_symbol_settings is not None
                    for symbol_key in group_symbol_settings.keys():
                        prices = await get_last_known_price(redis_client, symbol_key)
                        if prices:
                            adjusted_market_prices[symbol_key] = prices
                
                portfolio = await calculate_user_portfolio(user_data_for_portfolio, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                await set_user_portfolio_cache(redis_client, user_id, portfolio)
                await publish_account_structure_changed_event(redis_client, user_id)
        except Exception as e:
            orders_logger.error(f"[PENDING_ORDER] Error updating portfolio cache or publishing websocket event: {str(e)}", exc_info=True)
            # Continue execution - don't return here as the order is already opened
        
        # Remove the order from Redis pending list AFTER successful processing
        # Use the original_order_type for removal as that's how it's stored in Redis
        try:
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id)
            
            # Notify clients about order execution through websockets
            await publish_order_execution_notification(redis_client, user_id, order_id, symbol, new_order_type, exec_price)
        except Exception as remove_error:
            orders_logger.error(f"[PENDING_ORDER] Error removing order from Redis: {str(remove_error)}", exc_info=True)

    except Exception as e:
        orders_logger.error(f"[PENDING_ORDER] Critical error in trigger_pending_order for order {order.get('order_id', 'N/A')}: {str(e)}", exc_info=True)
        raise

# New function to publish order execution notification
async def publish_order_execution_notification(redis_client: Redis, user_id: str, order_id: str, symbol: str, order_type: str, execution_price: Decimal):
    """
    Publishes a notification that a pending order has been executed to the Redis pub/sub channel.
    This allows websocket clients to be notified of order execution in real-time.
    """
    try:
        from app.core.logging_config import orders_logger
        
        # Create the notification payload
        notification = {
            "event": "pending_order_executed",
            "user_id": user_id,
            "order_id": order_id,
            "symbol": symbol,
            "order_type": order_type,
            "execution_price": str(execution_price),
            "timestamp": datetime.now().isoformat()
        }
        
        # Publish to the user's channel
        channel = f"user:{user_id}:notifications"
        await redis_client.publish(channel, json.dumps(notification))
    
    except Exception as e:
        from app.core.logging_config import orders_logger
        orders_logger.error(f"[PENDING_ORDER] Error publishing order execution notification: {str(e)}", exc_info=True)

async def check_and_trigger_stoploss_takeprofit(
    db: AsyncSession,
    redis_client: Redis
) -> None:
    """
    Check all open orders for stop loss and take profit conditions.
    This function is called by the background task triggered by market data updates.
    Optimized to fetch open orders from cache if available.
    Only checks orders that have SL or TP set (>0).
    """
    logger = orders_logger
    try:
        from app.crud.user import get_all_active_users_both
        live_users, demo_users = await get_all_active_users_both(db)
        all_users = [(u.id, "live") for u in live_users] + [(u.id, "demo") for u in demo_users]
        for user_id, user_type in all_users:
            try:
                static_orders = await get_user_static_orders_cache(redis_client, user_id)
                open_orders = []
                if static_orders and static_orders.get("open_orders"):
                    open_orders = static_orders["open_orders"]
                else:
                    order_model = get_order_model(user_type)
                    open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                    for o in open_orders_orm:
                        open_orders.append({
                            'order_id': getattr(o, 'order_id', None),
                            'order_company_name': getattr(o, 'order_company_name', None),
                            'order_type': getattr(o, 'order_type', None),
                            'order_quantity': getattr(o, 'order_quantity', None),
                            'order_price': getattr(o, 'order_price', None),
                            'margin': getattr(o, 'margin', None),
                            'contract_value': getattr(o, 'contract_value', None),
                            'stop_loss': getattr(o, 'stop_loss', None),
                            'take_profit': getattr(o, 'take_profit', None),
                            'commission': getattr(o, 'commission', None),
                            'order_status': getattr(o, 'order_status', None),
                            'order_user_id': getattr(o, 'order_user_id', None)
                        })
                # Only check orders with SL or TP set (>0)
                def get_attr(o, key):
                    if isinstance(o, dict):
                        return o.get(key)
                    return getattr(o, key, None)
                sl_tp_orders = [
                    o for o in open_orders
                    if (get_attr(o, 'stop_loss') and Decimal(str(get_attr(o, 'stop_loss'))) > 0)
                    or (get_attr(o, 'take_profit') and Decimal(str(get_attr(o, 'take_profit'))) > 0)
                ]
                for order in sl_tp_orders:
                    try:
                        # Verify order has required fields
                        order_id = get_attr(order, 'order_id')
                        order_user_id = get_attr(order, 'order_user_id')
                        
                        if not order_id or not order_user_id:
                            logger.error(f"[SLTP_CHECK] Order {order_id or 'unknown'} has missing required fields: order_id={order_id}, order_user_id={order_user_id}")
                            continue
                            
                        # Verify user exists before processing
                        user = None
                        if user_type == 'live':
                            from app.crud.user import get_user_by_id
                            user = await get_user_by_id(db, order_user_id, user_type='live')
                            if not user:
                                # Try demo table if live lookup fails
                                from app.crud.user import get_demo_user_by_id
                                user = await get_demo_user_by_id(db, order_user_id)
                                if user:
                                    # If found in demo table, update user_type
                                    user_type = 'demo'
                        else:
                            from app.crud.user import get_demo_user_by_id
                            user = await get_demo_user_by_id(db, order_user_id)
                            if not user:
                                # Try live table if demo lookup fails
                                from app.crud.user import get_user_by_id
                                user = await get_user_by_id(db, order_user_id, user_type='live')
                                if user:
                                    # If found in live table, update user_type
                                    user_type = 'live'
                            
                        if not user:
                            logger.error(f"[SLTP_CHECK] Skipping order {order_id} - User {order_user_id} not found in either live or demo tables")
                            continue
                            
                        await process_order_stoploss_takeprofit(db, redis_client, order, user_type)
                    except Exception as e:
                        logger.error(f"[SLTP_CHECK] Error processing order {get_attr(order, 'order_id')}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"[SLTP_CHECK] Error processing user {user_id}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[SLTP_CHECK] Error in check_and_trigger_stoploss_takeprofit: {e}", exc_info=True)

async def process_order_stoploss_takeprofit(
    db: AsyncSession,
    redis_client: Redis,
    order,
    user_type: str
) -> None:
    """
    Process a single order for stop loss and take profit conditions.
    Enhanced with epsilon accuracy to handle floating-point precision issues.
    Accepts both ORM and dict order objects.
    """
    logger = orders_logger
    try:
        # Support both ORM and dict order objects
        def get_attr(o, key):
            if isinstance(o, dict):
                return o.get(key)
            return getattr(o, key, None)
            
        # Verify required fields
        order_id = get_attr(order, 'order_id')
        order_user_id = get_attr(order, 'order_user_id')
        symbol = get_attr(order, 'order_company_name')
        
        if not all([order_id, order_user_id, symbol]):
            logger.error(f"[SLTP_CHECK] Order {order_id or 'unknown'} missing required fields: order_id={order_id}, order_user_id={order_user_id}, symbol={symbol}")
            return
            
        # Get user's group name
        user = None
        if user_type == 'live':
            from app.crud.user import get_user_by_id
            user = await get_user_by_id(db, order_user_id, user_type='live')
            if not user:
                # Try demo table if live lookup fails
                from app.crud.user import get_demo_user_by_id
                user = await get_demo_user_by_id(db, order_user_id)
                if user:
                    # If found in demo table, update user_type
                    user_type = 'demo'
        else:
            from app.crud.user import get_demo_user_by_id
            user = await get_demo_user_by_id(db, order_user_id)
            if not user:
                # Try live table if demo lookup fails
                from app.crud.user import get_user_by_id
                user = await get_user_by_id(db, order_user_id, user_type='live')
                if user:
                    # If found in live table, update user_type for this order
                    user_type = 'live'

        if not user:
            logger.error(f"[SLTP_CHECK] User {order_user_id} not found in either live or demo tables for order {order_id}")
            return
        group_name = user.group_name
        if not group_name:
            logger.error(f"[SLTP_CHECK] No group name found for user {order_user_id}")
            return
        # Get adjusted market price
        adjusted_price = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
        if not adjusted_price:
            logger.warning(f"[SLTP_CHECK] No adjusted price available for {symbol} in group {group_name}")
            return
        buy_price = Decimal(str(adjusted_price.get('buy', '0')))
        sell_price = Decimal(str(adjusted_price.get('sell', '0')))
        epsilon = Decimal(SLTP_EPSILON)
        # Check stop loss
        stop_loss = get_attr(order, 'stop_loss')
        order_type = get_attr(order, 'order_type')
        take_profit = get_attr(order, 'take_profit')
        if stop_loss and Decimal(str(stop_loss)) > 0:
            stop_loss = Decimal(str(stop_loss))
            if order_type == 'BUY':
                price_diff = abs(sell_price - stop_loss)
                exact_match = sell_price <= stop_loss
                epsilon_match = price_diff < epsilon
                should_trigger = exact_match or epsilon_match
                if should_trigger:
                    trigger_reason = "exact match" if exact_match else "epsilon tolerance"
                    logger.warning(f"[SLTP_CHECK] Stop loss triggered for BUY order {order_id} at {sell_price} <= {stop_loss} (diff: {price_diff}, epsilon: {epsilon}, reason: {trigger_reason})")
                    await close_order(db, redis_client, order, sell_price, 'STOP_LOSS', user_type)
            elif order_type == 'SELL':
                price_diff = abs(buy_price - stop_loss)
                exact_match = buy_price >= stop_loss
                epsilon_match = price_diff < epsilon
                should_trigger = exact_match or epsilon_match
                if should_trigger:
                    trigger_reason = "exact match" if exact_match else "epsilon tolerance"
                    logger.warning(f"[SLTP_CHECK] Stop loss triggered for SELL order {order_id} at {buy_price} >= {stop_loss} (diff: {price_diff}, epsilon: {epsilon}, reason: {trigger_reason})")
                    await close_order(db, redis_client, order, buy_price, 'STOP_LOSS', user_type)
        # Check take profit
        if take_profit and Decimal(str(take_profit)) > 0:
            take_profit = Decimal(str(take_profit))
            if order_type == 'BUY':
                price_diff = abs(sell_price - take_profit)
                exact_match = sell_price >= take_profit
                epsilon_match = price_diff < epsilon
                should_trigger = exact_match or epsilon_match
                if should_trigger:
                    trigger_reason = "exact match" if exact_match else "epsilon tolerance"
                    logger.warning(f"[SLTP_CHECK] Take profit triggered for BUY order {order_id} at {sell_price} >= {take_profit} (diff: {price_diff}, epsilon: {epsilon}, reason: {trigger_reason})")
                    await close_order(db, redis_client, order, sell_price, 'TAKE_PROFIT', user_type)
            elif order_type == 'SELL':
                price_diff = abs(buy_price - take_profit)
                exact_match = buy_price <= take_profit
                epsilon_match = price_diff < epsilon
                should_trigger = exact_match or epsilon_match
                if should_trigger:
                    trigger_reason = "exact match" if exact_match else "epsilon tolerance"
                    logger.warning(f"[SLTP_CHECK] Take profit triggered for SELL order {order_id} at {buy_price} <= {take_profit} (diff: {price_diff}, epsilon: {epsilon}, reason: {trigger_reason})")
                    await close_order(db, redis_client, order, buy_price, 'TAKE_PROFIT', user_type)
    except Exception as e:
        logger.error(f"[SLTP_CHECK] Error processing order {get_attr(order, 'order_id')}: {e}", exc_info=True)

async def get_last_known_price(redis_client: Redis, symbol: str) -> dict:
    """
    Get the last known price for a symbol from Redis.
    Returns a dictionary with bid (b) and ask (a) prices.
    """
    try:
        if not redis_client:
            logger.warning("Redis client not available for getting last known price")
            return None
            
        # Try to get the price from Redis
        price_key = f"market_data:{symbol}"
        price_data = await redis_client.get(price_key)
        
        if not price_data:
            return None
            
        try:
            price_dict = json.loads(price_data)
            return price_dict
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in price data for {symbol}: {price_data}")
            return None
    except Exception as e:
        logger.error(f"Error getting last known price for {symbol}: {e}", exc_info=True)
        return None

async def get_user_data_cache(redis_client: Redis, user_id: int, db: AsyncSession, user_type: str = 'live') -> dict:
    """
    Get user data from cache or database.
    """
    try:
        if not redis_client:
            logger.warning(f"Redis client not available for getting user data for user {user_id}")
            # Fallback to database
            from app.crud.user import get_user_by_id, get_demo_user_by_id
            
            if user_type == 'live':
                user = await get_user_by_id(db, user_id, user_type=user_type)
            else:
                user = await get_demo_user_by_id(db, user_id)
                
            if not user:
                return {}
                
            return {
                "id": user.id,
                "group_name": getattr(user, 'group_name', None),
                "wallet_balance": str(user.wallet_balance) if hasattr(user, 'wallet_balance') else "0",
                "margin": str(user.margin) if hasattr(user, 'margin') else "0"
            }
        
        # Try to get from cache
        user_key = f"user_data:{user_type}:{user_id}"
        user_data = await redis_client.get(user_key)
        
        if user_data:
            try:
                return json.loads(user_data)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in user data for user {user_id}: {user_data}")
                return {}
        
        # Fallback to database if not in cache
        from app.crud.user import get_user_by_id, get_demo_user_by_id
        
        if user_type == 'live':
            user = await get_user_by_id(db, user_id, user_type=user_type)
        else:
            user = await get_demo_user_by_id(db, user_id)
            
        if not user:
            return {}
            
        user_data = {
            "id": user.id,
            "group_name": getattr(user, 'group_name', None),
            "wallet_balance": str(user.wallet_balance) if hasattr(user, 'wallet_balance') else "0",
            "margin": str(user.margin) if hasattr(user, 'margin') else "0"
        }
        
        # Cache the user data
        try:
            await redis_client.set(user_key, json.dumps(user_data), ex=300)  # 5 minutes expiry
        except Exception as e:
            logger.error(f"Error caching user data for user {user_id}: {e}", exc_info=True)
        
        return user_data
    except Exception as e:
        logger.error(f"Error getting user data for user {user_id}: {e}", exc_info=True)
        return {}

async def get_group_settings_cache(redis_client: Redis, group_name: str) -> dict:
    """
    Get group settings from cache or database.
    """
    try:
        if not group_name:
            return {}
            
        if not redis_client:
            logger.warning(f"Redis client not available for getting group settings for group {group_name}")
            # Fallback to database
            from app.crud.group import get_group_by_name
            from sqlalchemy.ext.asyncio import AsyncSession
            from app.database.session import AsyncSessionLocal
            
            async with AsyncSessionLocal() as db:
                group = await get_group_by_name(db, group_name)
                if not group:
                    return {}
                
                # Handle case where get_group_by_name returns a list
                group_obj = group[0] if isinstance(group, list) else group
                    
                return {
                    "id": group_obj.id,
                    "name": group_obj.name,
                    "sending_orders": getattr(group_obj, 'sending_orders', None)
                }
        
        # Try to get from cache
        group_key = f"group_settings:{group_name}"
        group_data = await redis_client.get(group_key)
        
        if group_data:
            try:
                return json.loads(group_data)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in group data for group {group_name}: {group_data}")
                return {}
        
        # Fallback to database if not in cache
        from app.crud.group import get_group_by_name
        from sqlalchemy.ext.asyncio import AsyncSession
        from app.database.session import AsyncSessionLocal
        
        async with AsyncSessionLocal() as db:
            group = await get_group_by_name(db, group_name)
            if not group:
                return {}
            
            # Handle case where get_group_by_name returns a list
            group_obj = group[0] if isinstance(group, list) else group
                
            group_data = {
                "id": group_obj.id,
                "name": group_obj.name,
                "sending_orders": getattr(group_obj, 'sending_orders', None)
            }
            
            # Cache the group data
            try:
                await redis_client.set(group_key, json.dumps(group_data), ex=300)  # 5 minutes expiry
            except Exception as e:
                logger.error(f"Error caching group data for group {group_name}: {e}", exc_info=True)
            
            return group_data
    except Exception as e:
        logger.error(f"Error getting group settings for group {group_name}: {e}", exc_info=True)
        return {}

async def update_user_static_orders(user_id: int, db: AsyncSession, redis_client: Redis, user_type: str):
    """
    Update the static orders cache for a user after order changes.
    This includes both open and pending orders.
    Always fetches fresh data from the database to ensure the cache is up-to-date.
    """
    try:
        order_model = get_order_model(user_type)
        
        # Get open orders - always fetch from database to ensure fresh data
        open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
        open_orders_data = []
        for pos in open_orders_orm:
            pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                       for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                   'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'order_user_id', 'order_status']}
            pos_dict['commission'] = str(getattr(pos, 'commission', '0.0'))
            open_orders_data.append(pos_dict)
        
        # Get pending orders - always fetch from database to ensure fresh data
        pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
        pending_orders_orm = await crud_order.get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model)
        pending_orders_data = []
        for po in pending_orders_orm:
            po_dict = {attr: str(v) if isinstance(v := getattr(po, attr, None), Decimal) else v
                      for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                  'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'order_user_id', 'order_status']}
            po_dict['commission'] = str(getattr(po, 'commission', '0.0'))
            pending_orders_data.append(po_dict)
        
        # Cache the static orders data
        static_orders_data = {
            "open_orders": open_orders_data,
            "pending_orders": pending_orders_data,
            "updated_at": datetime.now().isoformat()
        }
        await set_user_static_orders_cache(redis_client, user_id, static_orders_data)
        
        return static_orders_data
    except Exception as e:
        logger.error(f"Error updating static orders cache for user {user_id}: {e}", exc_info=True)
        return {"open_orders": [], "pending_orders": [], "updated_at": datetime.now().isoformat()}

async def close_order(
    db: AsyncSession,
    redis_client: Redis,
    order,
    execution_price: Decimal,
    close_reason: str,
    user_type: str
) -> None:
    """
    Close an order with the given execution price and reason.
    This function includes robust margin calculations, commission handling, and wallet transactions.
    """
    logger = orders_logger
    
    def get_attr(o, key):
        if isinstance(o, dict):
            return o.get(key)
        return getattr(o, key, None)
    
    try:
        # Extract all needed fields using get_attr
        order_id = get_attr(order, 'order_id')
        order_user_id = get_attr(order, 'order_user_id')
        order_company_name = get_attr(order, 'order_company_name')
        order_type = get_attr(order, 'order_type')
        order_status = get_attr(order, 'order_status')
        order_price = get_attr(order, 'order_price')
        order_quantity = get_attr(order, 'order_quantity')
        margin = get_attr(order, 'margin')
        contract_value = get_attr(order, 'contract_value')
        stop_loss = get_attr(order, 'stop_loss')
        take_profit = get_attr(order, 'take_profit')
        close_price = execution_price
        net_profit = get_attr(order, 'net_profit')
        swap = get_attr(order, 'swap')
        commission = get_attr(order, 'commission')
        cancel_message = get_attr(order, 'cancel_message')
        close_message = get_attr(order, 'close_message')
        cancel_id = get_attr(order, 'cancel_id')
        close_id = get_attr(order, 'close_id')
        modify_id = get_attr(order, 'modify_id')
        stoploss_id = get_attr(order, 'stoploss_id')
        takeprofit_id = get_attr(order, 'takeprofit_id')
        stoploss_cancel_id = get_attr(order, 'stoploss_cancel_id')
        takeprofit_cancel_id = get_attr(order, 'takeprofit_cancel_id')
        order_status = get_attr(order, 'order_status')

        # Always use ORM order object for margin and DB updates
        db_order_obj = order if not isinstance(order, dict) else None
        if db_order_obj is None:
            from app.crud.crud_order import get_order_by_id
            db_order_obj = await get_order_by_id(db, order_id, get_order_model(user_type))
            if db_order_obj is None:
                logger.error(f"[ORDER_CLOSE] Could not fetch ORM order object for order_id={order_id} (user_id={order_user_id})")
                return

        # Lock user for atomic operations
        if user_type == 'live':
            from app.crud.user import get_user_by_id_with_lock
            db_user_locked = await get_user_by_id_with_lock(db, order_user_id)
        else:
            from app.crud.user import get_demo_user_by_id_with_lock
            db_user_locked = await get_demo_user_by_id_with_lock(db, order_user_id)
        if db_user_locked is None:
            logger.error(f"[ORDER_CLOSE] Could not retrieve and lock user record for user ID: {order_user_id}")
            return

        # Get all open orders for this symbol to recalculate margin
        from app.crud.crud_order import get_open_orders_by_user_id_and_symbol, update_order_with_tracking
        order_model_class = get_order_model(user_type)
        all_open_orders_for_symbol = await get_open_orders_by_user_id_and_symbol(
            db=db, user_id=db_user_locked.id, symbol=order_company_name, order_model=order_model_class
        )

        # Calculate margin before closing this order
        margin_before_recalc_dict = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=db_user_locked.id,
            symbol=order_company_name,
            open_positions_for_symbol=all_open_orders_for_symbol,
            user_type=user_type,
            order_model=order_model_class
        )
        margin_before_recalc = margin_before_recalc_dict["total_margin"]
        current_overall_margin = Decimal(str(db_user_locked.margin))
        non_symbol_margin = current_overall_margin - margin_before_recalc

        # Calculate margin after closing this order
        remaining_orders_for_symbol_after_close = [
            o for o in all_open_orders_for_symbol
            if str(get_attr(o, 'order_id')) != str(order_id)
        ]
        margin_after_symbol_recalc_dict = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=db_user_locked.id,
            symbol=order_company_name,
            open_positions_for_symbol=remaining_orders_for_symbol_after_close,
            user_type=user_type,
            order_model=order_model_class
        )
        margin_after_symbol_recalc = margin_after_symbol_recalc_dict["total_margin"]

        # Update user's margin
        db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

        # Get contract size and profit currency from ExternalSymbolInfo
        from app.database.models import ExternalSymbolInfo
        from sqlalchemy.future import select
        symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_company_name))
        symbol_info_result = await db.execute(symbol_info_stmt)
        ext_symbol_info = symbol_info_result.scalars().first()
        if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
            logger.error(f"[ORDER_CLOSE] Missing critical ExternalSymbolInfo for symbol {order_company_name}.")
            return
        contract_size = Decimal(str(ext_symbol_info.contract_size))
        profit_currency = ext_symbol_info.profit.upper()

        # Get group settings for commission calculation
        from app.core.cache import get_group_symbol_settings_cache
        group_settings = await get_group_symbol_settings_cache(redis_client, db_user_locked.group_name, order_company_name)
        if not group_settings:
            logger.error("[ORDER_CLOSE] Group settings not found for commission calculation.")
            return
        commission_type = int(group_settings.get('commision_type', -1))
        commission_value_type = int(group_settings.get('commision_value_type', -1))
        commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
        existing_entry_commission = Decimal(str(commission or "0.0"))
        exit_commission = Decimal("0.0")
        quantity = Decimal(str(order_quantity))
        entry_price = Decimal(str(order_price))
        if commission_type in [0, 2]:
            if commission_value_type == 0:
                exit_commission = quantity * commission_rate
            elif commission_value_type == 1:
                calculated_exit_contract_value = quantity * contract_size * close_price
                if calculated_exit_contract_value > Decimal("0.0"):
                    exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
        total_commission_for_trade = (existing_entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Calculate profit/loss
        if order_type == "BUY":
            profit = (close_price - entry_price) * quantity * contract_size
        elif order_type == "SELL":
            profit = (entry_price - close_price) * quantity * contract_size
        else:
            logger.error("[ORDER_CLOSE] Invalid order type.")
            return
        profit_usd = await _convert_to_usd(profit, profit_currency, db_user_locked.id, order_id, "PnL on Close", db=db, redis_client=redis_client)
        if profit_currency != "USD" and profit_usd == profit:
            logger.error(f"[ORDER_CLOSE] Order {order_id}: PnL conversion failed. Rates missing for {profit_currency}/USD.")
            return
        from app.services.order_processing import generate_unique_10_digit_id
        close_id_val = await generate_unique_10_digit_id(db, order_model_class, 'close_id')

        # Update order fields on ORM object
        db_order_obj.order_status = "CLOSED"
        db_order_obj.close_price = close_price
        db_order_obj.net_profit = (profit_usd - total_commission_for_trade).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        db_order_obj.swap = swap or Decimal("0.0")
        db_order_obj.commission = total_commission_for_trade
        db_order_obj.close_id = close_id_val
        db_order_obj.close_message = f"Closed automatically due to {close_reason}"

        # Update order with tracking
        await update_order_with_tracking(
            db=db,
            db_order=db_order_obj,
            update_fields={
                "order_status": db_order_obj.order_status,
                "close_price": db_order_obj.close_price,
                "close_id": db_order_obj.close_id,
                "net_profit": db_order_obj.net_profit,
                "swap": db_order_obj.swap,
                "commission": db_order_obj.commission,
                "close_message": db_order_obj.close_message
            },
            user_id=db_user_locked.id,
            user_type=user_type,
            action_type=f"AUTO_{close_reason.upper()}_CLOSE"
        )

        # Update user's wallet balance
        original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
        swap_amount = db_order_obj.swap
        db_user_locked.wallet_balance = (original_wallet_balance + db_order_obj.net_profit - swap_amount).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

        # Create wallet transactions
        from app.database.models import Wallet
        from app.schemas.wallet import WalletCreate
        transaction_time = datetime.now(timezone.utc)
        wallet_common_data = {
            "symbol": order_company_name,
            "order_quantity": quantity,
            "is_approved": 1,
            "order_type": order_type,
            "transaction_time": transaction_time,
            "order_id": str(order_id)
        }
        if user_type == 'demo':
            wallet_common_data["demo_user_id"] = db_user_locked.id
        else:
            wallet_common_data["user_id"] = db_user_locked.id
        if db_order_obj.net_profit != Decimal("0.0"):
            transaction_id_profit = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=db_order_obj.net_profit, description=f"P/L for closing order {order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_profit))
        if total_commission_for_trade > Decimal("0.0"):
            transaction_id_commission = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Commission for closing order {order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_commission))
        if swap_amount != Decimal("0.0"):
            transaction_id_swap = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap for closing order {order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_swap))

        await db.commit()
        await db.refresh(db_order_obj)
        await db.refresh(db_user_locked)
        logger.info(f"[ORDER_CLOSE] Successfully closed order {order_id} for user {order_user_id}")

        # --- Websocket and cache updates ---
        user_type_str = 'demo' if user_type == 'demo' else 'live'
        user_data_to_cache = {
            "id": db_user_locked.id,
            "email": getattr(db_user_locked, 'email', None),
            "group_name": db_user_locked.group_name,
            "leverage": db_user_locked.leverage,
            "user_type": user_type_str,
            "account_number": getattr(db_user_locked, 'account_number', None),
            "wallet_balance": db_user_locked.wallet_balance,
            "margin": db_user_locked.margin,
            "first_name": getattr(db_user_locked, 'first_name', None),
            "last_name": getattr(db_user_locked, 'last_name', None),
            "country": getattr(db_user_locked, 'country', None),
            "phone_number": getattr(db_user_locked, 'phone_number', None),
        }
        from app.api.v1.endpoints.market_data_ws import update_static_orders_cache
        # await set_user_data_cache(redis_client, db_user_locked.id, user_data_to_cache)
        # from app.api.v1.endpoints.orders import update_user_static_orders, publish_order_update, publish_user_data_update, publish_market_data_trigger
        # await update_user_static_orders(db_user_locked.id, db, redis_client, user_type_str)
        # await publish_order_update(redis_client, db_user_locked.id)
        # await publish_user_data_update(redis_client, db_user_locked.id)
        # await publish_market_data_trigger(redis_client)
        await set_user_data_cache(redis_client, order_user_id, user_data_to_cache, user_type)
        
        # Update balance/margin cache for websocket
        await set_user_balance_margin_cache(redis_client, order_user_id, db_user_locked.wallet_balance, db_user_locked.margin)
        logger.info(f"Balance/margin cache updated for user {order_user_id}: balance={db_user_locked.wallet_balance}, margin={db_user_locked.margin}")
        
        await update_static_orders_cache(order_user_id, db, redis_client, user_type)
        await publish_order_update(redis_client, order_user_id)
        await publish_user_data_update(redis_client, order_user_id)
    except Exception as e:
        logger.error(f"[ORDER_CLOSE] Error closing order {get_attr(order, 'order_id')}: {e}", exc_info=True)

        