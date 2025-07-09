# app/main.py

# --- Environment Variable Loading ---
# This must be at the very top, before any other app modules are imported.
from dotenv import load_dotenv
load_dotenv()

# Import necessary components from fastapi
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import os
import json
from typing import Optional, Any
from datetime import datetime
from decimal import Decimal
from redis.asyncio import Redis

import logging

# --- Trading Configuration ---
# Epsilon value for SL/TP accuracy (floating-point precision tolerance)
# For forex (5 decimal places), use 0.00001 as tolerance
SLTP_EPSILON = Decimal('0.00001')

# --- APScheduler Imports ---
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# --- Custom Service and DB Session for Scheduler ---
from app.services.swap_service import apply_daily_swap_charges_for_all_open_orders
from app.database.session import AsyncSessionLocal

# Import portfolio calculator
from app.services.portfolio_calculator import calculate_user_portfolio
from app.core.cache import (
    set_user_data_cache,
    get_user_data_cache, 
    get_group_symbol_settings_cache, 
    get_adjusted_market_price_cache, 
    set_user_dynamic_portfolio_cache,
    get_last_known_price,
    publish_order_update,
    publish_user_data_update,
    publish_market_data_trigger,
    REDIS_MARKET_DATA_CHANNEL,
    decode_decimal
)
from app.crud import crud_order, user as crud_user
from app.core.firebase import send_order_to_firebase

# --- CORS Middleware Import ---
from fastapi.middleware.cors import CORSMiddleware

# Import pre-configured loggers from logging_config
from app.core.logging_config import (
    orders_logger, 
    autocutoff_logger, 
    app_logger,
    database_logger,
    firebase_logger,
    redis_logger,
    security_logger,
    market_data_logger,
    cache_logger,
    frontend_orders_logger,
    service_provider_logger,
    firebase_comm_logger,
    orders_crud_logger,
    jwt_security_logger,
    error_logger,
    money_requests_logger,
    websocket_logger
)

# Use the main app logger for this module
logger = app_logger

# Import Firebase Admin SDK components
import firebase_admin
from firebase_admin import credentials, db as firebase_db

# Import configuration settings
from app.core.config import get_settings

# Import database session dependency and table creation function
from app.database.session import get_db, create_all_tables

# Import API router
from app.api.v1.api import api_router

# Import background tasks
from app.firebase_stream import process_firebase_events
# REMOVE: from app.api.v1.endpoints.market_data_ws import redis_market_data_broadcaster
from app.api.v1.endpoints.market_data_ws import redis_publisher_task # Keep publisher

# Import Redis dependency and global instance
from app.dependencies.redis_client import get_redis_client, global_redis_client_instance
from app.core.security import close_redis_connection, create_service_account_token

# Import shared state (for the queue)
from app.shared_state import redis_publish_queue

# Import order processing utilities
from app.services.order_processing import generate_unique_10_digit_id
from app.database.models import UserOrder, DemoUser

# Import stop loss and take profit checker
from app.services.pending_orders import check_and_trigger_stoploss_takeprofit

# Import adjusted price worker
from app.services.adjusted_price_worker import adjusted_price_worker

settings = get_settings()
app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# --- CORS Settings ---
# Define specific origins for better security
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:5500",
    "http://localhost:8000",
    "http://localhost:8080",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5500",
    "http://127.0.0.1:8000",
    "http://127.0.0.1:8080",
    # Add your production domains here
    "https://yourdomain.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Use specific origins
    allow_credentials=False,  # Allow credentials
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["*"],
    expose_headers=["Content-Type", "Authorization", "X-Total-Count"]
)
# --- End CORS Settings ---

scheduler: Optional[AsyncIOScheduler] = None

# Now, you can safely print and access them
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")

print(f"--- Application Startup ---")
print(f"Loaded SECRET_KEY (from code): '{SECRET_KEY}'")
print(f"Loaded ALGORITHM (from code): '{ALGORITHM}'")
print(f"---------------------------")

# Log application startup
orders_logger.info("Application starting up - Orders logging initialized")
orders_logger.info(f"SL/TP Epsilon accuracy configured: {SLTP_EPSILON}")

# --- Scheduled Job Functions ---
async def daily_swap_charge_job():
    logger.info("APScheduler: Executing daily_swap_charge_job...")
    async with AsyncSessionLocal() as db:
        if global_redis_client_instance:
            try:
                await apply_daily_swap_charges_for_all_open_orders(db, global_redis_client_instance)
                logger.info("APScheduler: Daily swap charge job completed successfully.")
            except Exception as e:
                logger.error(f"APScheduler: Error during daily_swap_charge_job: {e}", exc_info=True)
        else:
            logger.error("APScheduler: Cannot execute daily_swap_charge_job - Global Redis client not available.")

# --- New Dynamic Portfolio Update Job ---
async def update_all_users_dynamic_portfolio():
    """
    Background task that updates the dynamic portfolio data (free_margin, margin_level)
    for all users, regardless of whether they are connected via WebSockets.
    This is critical for autocutoff and validation.
    """
    try:
        logger.debug("Starting update_all_users_dynamic_portfolio job")
        async with AsyncSessionLocal() as db:
            if not global_redis_client_instance:
                logger.error("Cannot update dynamic portfolios - Redis client not available")
                return
                
            # Get all active users (both live and demo) using the new unified function
            live_users, demo_users = await crud_user.get_all_active_users_both(db)
            
            all_users = []
            for user in live_users:
                all_users.append({"id": user.id, "user_type": "live", "group_name": user.group_name})
            for user in demo_users:
                all_users.append({"id": user.id, "user_type": "demo", "group_name": user.group_name})
            
            logger.debug(f"Found {len(all_users)} active users to update portfolios")
            
            # Process each user
            for user_info in all_users:
                user_id = user_info["id"]
                user_type = user_info["user_type"]
                group_name = user_info["group_name"]
                
                try:
                    # Get user data from cache or DB
                    user_data = await get_user_data_cache(global_redis_client_instance, user_id, db, user_type)
                    if not user_data:
                        logger.warning(f"No user data found for user {user_id} ({user_type}). Skipping portfolio update.")
                        continue
                    
                    # Get group symbol settings
                    if not group_name:
                        logger.warning(f"User {user_id} has no group_name set. Skipping portfolio update.")
                        continue
                    group_symbol_settings = await get_group_symbol_settings_cache(global_redis_client_instance, group_name, "ALL")
                    if not group_symbol_settings:
                        logger.warning(f"No group settings found for group {group_name}. Skipping portfolio update for user {user_id}.")
                        continue
                    
                    # Get open orders for this user
                    order_model = crud_order.get_order_model(user_type)
                    open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                    open_positions = []
                    for o in open_orders_orm:
                        open_positions.append({
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
                    
                    if not open_positions:
                        # Skip portfolio calculation for users without open positions
                        continue
                    
                    # Get adjusted market prices for all relevant symbols
                    adjusted_market_prices = {}
                    for symbol in group_symbol_settings.keys():
                        # Try to get adjusted prices from cache
                        adjusted_prices = await get_adjusted_market_price_cache(global_redis_client_instance, group_name, symbol)
                        if adjusted_prices:
                            adjusted_market_prices[symbol] = {
                                'buy': adjusted_prices.get('buy'),
                                'sell': adjusted_prices.get('sell')
                            }
                        else:
                            # Fallback to last known price
                            last_price = await get_last_known_price(global_redis_client_instance, symbol)
                            if last_price:
                                adjusted_market_prices[symbol] = {
                                    'buy': last_price.get('b'),  # Use raw price as fallback
                                    'sell': last_price.get('o')
                                }
                    
                    # Define margin thresholds based on group settings or defaults
                    margin_call_threshold = Decimal('100.0')  # Default 100%
                    margin_cutoff_threshold = Decimal('50.0')  # Default 50%
                    
                    # Calculate portfolio metrics with margin call detection
                    portfolio_metrics = await calculate_user_portfolio(
                        user_data=user_data,
                        open_positions=open_positions,
                        adjusted_market_prices=adjusted_market_prices,
                        group_symbol_settings=group_symbol_settings,
                        redis_client=global_redis_client_instance,
                        margin_call_threshold=margin_call_threshold
                    )
                    
                    # Cache the dynamic portfolio data
                    dynamic_portfolio_data = {
                        "balance": portfolio_metrics.get("balance", "0.0"),
                        "equity": portfolio_metrics.get("equity", "0.0"),
                        "margin": portfolio_metrics.get("margin", "0.0"),
                        "free_margin": portfolio_metrics.get("free_margin", "0.0"),
                        "profit_loss": portfolio_metrics.get("profit_loss", "0.0"),
                        "margin_level": portfolio_metrics.get("margin_level", "0.0"),
                        "positions_with_pnl": portfolio_metrics.get("positions", []),
                        "margin_call": portfolio_metrics.get("margin_call", False)
                    }
                    await set_user_dynamic_portfolio_cache(global_redis_client_instance, user_id, dynamic_portfolio_data)
                    
                    # Check for margin call conditions
                    margin_level = Decimal(portfolio_metrics.get("margin_level", "0.0"))
                    if margin_level > Decimal('0') and margin_level < margin_cutoff_threshold:
                        autocutoff_logger.warning(f"[AUTO-CUTOFF] User {user_id} margin level {margin_level}% below cutoff threshold {margin_cutoff_threshold}%. Initiating auto-cutoff.")
                        await handle_margin_cutoff(db, global_redis_client_instance, user_id, user_type, margin_level)
                    elif portfolio_metrics.get("margin_call", False):
                        autocutoff_logger.warning(f"[AUTO-CUTOFF] User {user_id} has margin call condition: margin level {margin_level}%")
                    
                    # After portfolio update or order execution, log details if relevant
                    # orders_logger.info(f"[PENDING_ORDER_EXECUTION][PORTFOLIO_UPDATE] user_id={user_id}, user_type={user_type}, group_name={group_name}, free_margin={dynamic_portfolio_data.get('free_margin', 'N/A')}, margin_level={dynamic_portfolio_data.get('margin_level', 'N/A')}, balance={dynamic_portfolio_data.get('balance', 'N/A')}, equity={dynamic_portfolio_data.get('equity', 'N/A')}")
                    
                except Exception as user_error:
                    logger.error(f"Error updating portfolio for user {user_id}: {user_error}", exc_info=True)
                    continue
            
            logger.debug("Finished update_all_users_dynamic_portfolio job")
    except Exception as e:
        logger.error(f"Error in update_all_users_dynamic_portfolio job: {e}", exc_info=True)

# --- Auto-cutoff function for margin calls ---
async def handle_margin_cutoff(db: AsyncSession, redis_client: Redis, user_id: int, user_type: str, margin_level: Decimal):
    """
    Handles auto-cutoff for users whose margin level falls below the critical threshold.
    """
    try:
        is_barclays_live_user = False
        user_for_cutoff = None
        if user_type == "live":
            user_for_cutoff = await crud_user.get_user_by_id(db, user_id=user_id)
            if user_for_cutoff and user_for_cutoff.group_name:
                group_settings = await get_group_symbol_settings_cache(redis_client, user_for_cutoff.group_name)
                if group_settings.get('sending_orders', '').lower() == 'barclays':
                    is_barclays_live_user = True
        else:
            user_for_cutoff = await crud_user.get_demo_user_by_id(db, user_id)

        if not user_for_cutoff:
            return

        order_model = crud_order.get_order_model(user_type)
        open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)

        if not open_orders:
            return

        if is_barclays_live_user:
            for order in open_orders:
                try:
                    close_id = await generate_unique_10_digit_id(db, UserOrder, 'close_id')
                    
                    firebase_close_data = {
                        "action": "close_order",
                        "close_id": close_id,
                        "order_id": order.order_id,
                        "user_id": user_id,
                        "symbol": order.order_company_name,
                        "order_type": order.order_type,
                        "order_status": order.order_status,
                        "status": "close",
                        "order_quantity": str(order.order_quantity),
                        "contract_value": str(order.contract_value),
                        "timestamp": datetime.now(datetime.timezone.utc).isoformat(),
                    }
                    
                    await send_order_to_firebase(firebase_close_data, "live")
                    
                    update_fields = {
                        "close_id": close_id,
                        "close_message": f"Auto-cutoff triggered at margin level {margin_level}%. Close request sent to provider."
                    }
                    await crud_order.update_order_with_tracking(
                        db, order, update_fields, user_id, user_type, "AUTO_CUTOFF_REQUESTED"
                    )
                    await db.commit()

                except Exception:
                    continue
            
            await publish_order_update(redis_client, user_id)
            
            user_type_str = 'live'
            user_data_to_cache = {
                "id": user_for_cutoff.id,
                "email": getattr(user_for_cutoff, 'email', None),
                "group_name": user_for_cutoff.group_name,
                "leverage": user_for_cutoff.leverage,
                "user_type": user_type_str,
                "account_number": getattr(user_for_cutoff, 'account_number', None),
                "wallet_balance": user_for_cutoff.wallet_balance,
                "margin": user_for_cutoff.margin,
                "first_name": getattr(user_for_cutoff, 'first_name', None),
                "last_name": getattr(user_for_cutoff, 'last_name', None),
                "country": getattr(user_for_cutoff, 'country', None),
                "phone_number": getattr(user_for_cutoff, 'phone_number', None)
            }
            await set_user_data_cache(redis_client, user_id, user_data_to_cache)
            
            from app.api.v1.endpoints.orders import update_user_static_orders
            await update_user_static_orders(user_id, db, redis_client, user_type_str)
            await publish_user_data_update(redis_client, user_id)
            await publish_market_data_trigger(redis_client)

        else:
            from app.crud.external_symbol_info import get_external_symbol_info_by_symbol
            from app.services.portfolio_calculator import _convert_to_usd
            from app.services.order_processing import calculate_total_symbol_margin_contribution
            from app.database.models import ExternalSymbolInfo
            from sqlalchemy import select
            from app.schemas.wallet import WalletCreate
            from app.crud.wallet import generate_unique_10_digit_id
            from app.database.models import Wallet
            from decimal import ROUND_HALF_UP
            import datetime

            total_net_profit = Decimal('0.0')

            for order in open_orders:
                try:
                    symbol = order.order_company_name
                    last_price = await get_last_known_price(redis_client, symbol)
                    
                    if not last_price:
                        continue

                    close_price_str = last_price.get('o') if order.order_type == 'BUY' else last_price.get('b')
                    close_price = Decimal(str(close_price_str))

                    if not close_price or close_price <= 0:
                        continue
                    
                    close_id = await generate_unique_10_digit_id(db, order_model, 'close_id')
                    
                    quantity = Decimal(str(order.order_quantity))
                    entry_price = Decimal(str(order.order_price))
                    order_type_db = order.order_type.upper()
                    
                    symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
                    symbol_info_result = await db.execute(symbol_info_stmt)
                    ext_symbol_info = symbol_info_result.scalars().first()
                    
                    if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
                        continue
                    
                    contract_size = Decimal(str(ext_symbol_info.contract_size))
                    profit_currency = ext_symbol_info.profit.upper()
                    
                    group_settings = await get_group_symbol_settings_cache(redis_client, user_for_cutoff.group_name, symbol)
                    if not group_settings:
                        continue
                    
                    commission_type = int(group_settings.get('commision_type', -1))
                    commission_value_type = int(group_settings.get('commision_value_type', -1))
                    commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
                    
                    existing_entry_commission = Decimal(str(order.commission or "0.0"))
                    
                    exit_commission = Decimal("0.0")
                    if commission_type in [0, 2]:
                        if commission_value_type == 0:
                            exit_commission = quantity * commission_rate
                        elif commission_value_type == 1:
                            calculated_exit_contract_value = quantity * contract_size * close_price
                            if calculated_exit_contract_value > Decimal("0.0"):
                                exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
                    
                    total_commission_for_trade = (existing_entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    
                    if order_type_db == "BUY":
                        profit = (close_price - entry_price) * quantity * contract_size
                    elif order_type_db == "SELL":
                        profit = (entry_price - close_price) * quantity * contract_size
                    else:
                        continue
                    
                    profit_usd = await _convert_to_usd(profit, profit_currency, user_for_cutoff.id, order.order_id, "PnL on Auto-Cutoff", db=db, redis_client=redis_client)
                    if profit_currency != "USD" and profit_usd == profit:
                        continue
                    
                    net_profit = (profit_usd - total_commission_for_trade).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    
                    swap_amount = order.swap or Decimal("0.0")
                    
                    order.close_price = close_price
                    order.order_status = 'CLOSED'
                    order.close_message = f"Auto-cutoff: margin level {margin_level}%"
                    order.net_profit = net_profit
                    order.commission = total_commission_for_trade
                    order.close_id = close_id
                    order.swap = swap_amount
                    
                    total_net_profit += (net_profit - swap_amount)
                    
                    transaction_time = datetime.datetime.now(datetime.timezone.utc)
                    wallet_common_data = {
                        "symbol": symbol,
                        "order_quantity": quantity,
                        "is_approved": 1,
                        "order_type": order.order_type,
                        "transaction_time": transaction_time,
                        "order_id": order.order_id
                    }
                    
                    if isinstance(user_for_cutoff, DemoUser):
                        wallet_common_data["demo_user_id"] = user_for_cutoff.id
                    else:
                        wallet_common_data["user_id"] = user_for_cutoff.id
                    
                    if net_profit != Decimal("0.0"):
                        transaction_id_profit = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                        db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=net_profit, description=f"P/L for auto-cutoff order {order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_profit))
                    
                    if total_commission_for_trade > Decimal("0.0"):
                        transaction_id_commission = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                        db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Commission for auto-cutoff order {order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_commission))
                    
                    if swap_amount != Decimal("0.0"):
                        transaction_id_swap = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                        db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap for auto-cutoff order {order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_swap))

                except Exception:
                    continue

            try:
                remaining_open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                
                new_total_margin = Decimal('0.0')
                for remaining_order in remaining_open_orders:
                    symbol = remaining_order.order_company_name
                    symbol_orders = await crud_order.get_open_orders_by_user_id_and_symbol(db, user_id, symbol, order_model)
                    margin_data = await calculate_total_symbol_margin_contribution(
                        db, redis_client, user_id, symbol, symbol_orders, order_model, user_type
                    )
                    new_total_margin += margin_data["total_margin"]
                
                original_wallet_balance = Decimal(str(user_for_cutoff.wallet_balance))
                user_for_cutoff.wallet_balance = (original_wallet_balance + total_net_profit).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
                user_for_cutoff.margin = new_total_margin.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                
                await db.commit()
                
                user_type_str = 'demo' if isinstance(user_for_cutoff, DemoUser) else 'live'
                user_data_to_cache = {
                    "id": user_for_cutoff.id,
                    "email": getattr(user_for_cutoff, 'email', None),
                    "group_name": user_for_cutoff.group_name,
                    "leverage": user_for_cutoff.leverage,
                    "user_type": user_type_str,
                    "account_number": getattr(user_for_cutoff, 'account_number', None),
                    "wallet_balance": user_for_cutoff.wallet_balance,
                    "margin": user_for_cutoff.margin,
                    "first_name": getattr(user_for_cutoff, 'first_name', None),
                    "last_name": getattr(user_for_cutoff, 'last_name', None),
                    "country": getattr(user_for_cutoff, 'country', None),
                    "phone_number": getattr(user_for_cutoff, 'phone_number', None)
                }
                await set_user_data_cache(redis_client, user_id, user_data_to_cache)
                
                from app.api.v1.endpoints.orders import update_user_static_orders
                await update_user_static_orders(user_id, db, redis_client, user_type_str)
                await publish_order_update(redis_client, user_id)
                await publish_user_data_update(redis_client, user_id)
                await publish_market_data_trigger(redis_client)
                
            except Exception:
                await db.rollback()
            
    except Exception:
        pass

# --- Service Provider JWT Rotation Job ---
async def rotate_service_account_jwt():
    """
    Generates a JWT for the Barclays service provider and pushes it to Firebase.
    This job is scheduled to run periodically.
    """
    try:
        service_name = "barclays_service_provider"
        # Generate a token valid for 35 minutes. It will be refreshed every 30 minutes.
        token = create_service_account_token(service_name, expires_minutes=35)

        # Path in Firebase to store the token
        jwt_ref = firebase_db.reference(f"service_provider_credentials/{service_name}")
        
        # Payload to store in Firebase
        payload = {
            "jwt": token,
            "updated_at": datetime.utcnow().isoformat()
        }
        jwt_ref.set(payload)
        
        logger.info(f"Service account JWT for '{service_name}' was generated and pushed to Firebase.")

    except Exception as e:
        logger.error(f"Error in rotate_service_account_jwt job: {e}")

# Add this line after the app initialization
background_tasks = set()

@app.on_event("startup")
async def startup_event():
    global scheduler
    global background_tasks
    global global_redis_client_instance
    logger.info("Application startup initiated")
    import redis.asyncio as redis

    # r = redis.Redis(host="127.0.0.1", port=6379)
    # await r.flushall()
    # print("Redis flushed")
    # Print Redis connection info for debugging
    # redis_url = os.getenv("REDIS_URL")
    # redis_host = os.getenv("REDIS_HOST")
    # redis_port = os.getenv("REDIS_PORT")
    # redis_password = os.getenv("REDIS_PASSWORD")
    # print(f"[DEBUG] Redis connection info:")
    # print(f"  REDIS_URL: {redis_url}")
    # print(f"  REDIS_HOST: {redis_host}")
    # print(f"  REDIS_PORT: {redis_port}")
    # print(f"  REDIS_PASSWORD: {redis_password}")

    # Initialize Firebase

    try:
        cred_path = os.path.join(os.path.dirname(__file__), '..', settings.FIREBASE_SERVICE_ACCOUNT_KEY_PATH)
        if not os.path.exists(cred_path):
            raise FileNotFoundError("Firebase credentials file not found")
            
        cred = credentials.Certificate(cred_path)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred, {
                'databaseURL': settings.FIREBASE_DATABASE_URL
            })
            logger.info("Firebase initialized")
        else:
            logger.info("Firebase already initialized")
    except Exception as e:
        logger.error("Firebase initialization error")
    
    # Initialize Redis connection pool
    redis_available = False
    try:
        redis_client = await get_redis_client()
        if redis_client:
            ping_result = await redis_client.ping()
            if ping_result:
                redis_available = True
                global_redis_client_instance = redis_client
                logger.info("Redis initialized")
                # --- Print all Redis keys at startup ---
                # try:
                #     keys = await redis_client.keys("*")
                #     print(f"[STARTUP] Redis contains {len(keys)} keys:")
                #     for k in keys:
                #         print(f"  - {k}")
                # except Exception as e:
                #     print(f"[STARTUP] Error fetching Redis keys: {e}")
    except Exception:
        logger.warning("Redis initialization failed")
    
    # Initialize APScheduler
    try:
        scheduler = AsyncIOScheduler()
        
        scheduler.add_job(
            daily_swap_charge_job,
            CronTrigger(hour=0, minute=5),
            id='daily_swap_charge_job',
            replace_existing=True
        )
        
        scheduler.add_job(
            update_all_users_dynamic_portfolio,
            IntervalTrigger(minutes=1),
            id='update_all_users_dynamic_portfolio',
            replace_existing=True
        )
        
        scheduler.add_job(
            rotate_service_account_jwt,
            IntervalTrigger(minutes=30),
            id='rotate_service_account_jwt',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("Scheduler initialized")
    except Exception:
        logger.error("Scheduler initialization error")
    
    # Start background tasks
    try:
        firebase_task = asyncio.create_task(process_firebase_events(firebase_db, path=settings.FIREBASE_DATA_PATH))
        background_tasks.add(firebase_task)
        firebase_task.add_done_callback(background_tasks.discard)
        
        if redis_available and global_redis_client_instance:
            redis_task = asyncio.create_task(redis_publisher_task(global_redis_client_instance))
            background_tasks.add(redis_task)
            redis_task.add_done_callback(background_tasks.discard)
            
            # Start the centralized adjusted price worker
            adjusted_price_task = asyncio.create_task(adjusted_price_worker(global_redis_client_instance))
            background_tasks.add(adjusted_price_task)
            adjusted_price_task.add_done_callback(background_tasks.discard)
            
            pending_orders_task = asyncio.create_task(run_pending_order_checker())
            background_tasks.add(pending_orders_task)
            pending_orders_task.add_done_callback(background_tasks.discard)
            
            sltp_task = asyncio.create_task(run_sltp_checker_on_market_update())
            background_tasks.add(sltp_task)
            sltp_task.add_done_callback(background_tasks.discard)
            
            redis_cleanup_task = asyncio.create_task(cleanup_orphaned_redis_orders())
            background_tasks.add(redis_cleanup_task)
            redis_cleanup_task.add_done_callback(background_tasks.discard)
            
        logger.info("Background tasks initialized")
            
    except Exception:
        logger.error("Background tasks initialization error")
    
    # Create initial service account token
    try:
        await rotate_service_account_jwt()
    except Exception:
        logger.error("Initial service account token creation failed")
    
    logger.info("Application startup completed")

@app.on_event("shutdown")
async def shutdown_event():
    global scheduler, global_redis_client_instance
    logger.info("Application shutdown initiated")

    if scheduler and scheduler.running:
        try:
            scheduler.shutdown(wait=True)
            logger.info("Scheduler shutdown completed")
        except Exception:
            logger.error("Scheduler shutdown error")

    for task in list(background_tasks):
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.error("Background task cancellation error")

    if global_redis_client_instance:
        await close_redis_connection(global_redis_client_instance)
        global_redis_client_instance = None
        logger.info("Redis connection closed")

    from app.firebase_stream import cleanup_firebase
    cleanup_firebase()

    logger.info("Application shutdown completed")

app.include_router(api_router, prefix=settings.API_V1_STR)

@app.get("/")
async def read_root():
    return {"message": "Welcome to the Trading App Backend!"}

async def run_stoploss_takeprofit_checker():
    """Background task to continuously check for stop loss and take profit conditions"""
    logger = orders_logger
    
    from sqlalchemy.ext.asyncio import AsyncSession
    from app.database.session import AsyncSessionLocal
    
    logger.info("Starting stop loss/take profit checker background task")
    
    while True:
        try:
            # Create a new session for each check
            try:
                async with AsyncSessionLocal() as db:
                    # Get Redis client
                    try:
                        redis_client = await get_redis_client()
                        if not redis_client:
                            logger.warning("Redis client not available for SL/TP check - skipping this cycle")
                            await asyncio.sleep(10)  # Wait longer if Redis is not available
                            continue
                        
                        # Run the check
                        from app.services.pending_orders import check_and_trigger_stoploss_takeprofit
                        await check_and_trigger_stoploss_takeprofit(db, redis_client)
                    except Exception as e:
                        logger.error(f"Error getting Redis client for SL/TP check: {e}", exc_info=True)
                        await asyncio.sleep(10)  # Wait longer if there was an error
                        continue
            except Exception as session_error:
                logger.error(f"Error creating database session: {session_error}", exc_info=True)
                await asyncio.sleep(10)  # Wait longer if there was a session error
                continue
                
            # Sleep for a short time before the next check
            await asyncio.sleep(5)  # Check every 5 seconds
            
        except Exception as e:
            logger.error(f"Error in stop loss/take profit checker: {e}", exc_info=True)
            await asyncio.sleep(10)  # Wait longer if there was an error

# --- New Pending Order Checker Task ---
async def run_pending_order_checker():
    """
    Continuously runs the pending order checker in the background.
    SL/TP checks are now handled separately via market data updates.
    """
    logger = orders_logger

    await asyncio.sleep(5)
    logger.info("Starting the pending order checker background task.")
    
    while True:
        try:
            async with AsyncSessionLocal() as db:
                if global_redis_client_instance:
                    from app.crud import group as crud_group
                    all_groups = await crud_group.get_groups(db, skip=0, limit=1000)
                    
                    for group in all_groups:
                        group_name = group.name
                        group_settings = await get_group_symbol_settings_cache(global_redis_client_instance, group_name, "ALL")
                        
                        if not group_settings:
                            continue
                            
                        for symbol in group_settings.keys():
                            try:
                                adjusted_prices = await get_adjusted_market_price_cache(global_redis_client_instance, group_name, symbol)
                                
                                if adjusted_prices:
                                    from app.api.v1.endpoints.market_data_ws import check_and_trigger_pending_orders
                                    await check_and_trigger_pending_orders(
                                        redis_client=global_redis_client_instance,
                                        db=db,
                                        symbol=symbol,
                                        adjusted_prices=adjusted_prices,
                                        group_name=group_name
                                    )
                            except Exception as symbol_error:
                                logger.error(f"Error processing symbol {symbol}")
                                continue
                else:
                    await asyncio.sleep(5)
                    continue
                    
        except Exception as e:
            logger.error("Error in pending order checker loop")
            await asyncio.sleep(5)
            continue
        
        await asyncio.sleep(1)

# --- New SL/TP Checker Task (triggered by market data updates) ---
async def run_sltp_checker_on_market_update():
    """
    SL/TP checker that runs only when market data updates are received.
    This ensures SL/TP checks happen on every price tick.
    """
    logger = orders_logger

    # Give the application a moment to initialize everything else
    await asyncio.sleep(5) 
    logger.info("Starting the SL/TP checker task (triggered by market updates).")
    
    # Subscribe to market data updates
    pubsub = global_redis_client_instance.pubsub()
    await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
    
    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message is None:
                continue
                
            try:
                message_data = json.loads(message['data'], object_hook=decode_decimal)
                if message_data.get("type") == "market_data_update":
                    logger.info("Market data update received, triggering SL/TP check")
                    
                    # Run SL/TP check with fresh database session
                    async with AsyncSessionLocal() as db:
                        await check_and_trigger_stoploss_takeprofit(db, global_redis_client_instance)
                        
            except Exception as e:
                logger.error(f"Error processing market data for SL/TP check: {e}", exc_info=True)
                
    except Exception as e:
        logger.error(f"Error in SL/TP checker task: {e}", exc_info=True)
    finally:
        await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
        await pubsub.close()

# --- Redis Cleanup Function ---
async def cleanup_orphaned_redis_orders():
    """
    Periodically clean up orphaned orders in Redis that no longer exist in the database.
    """
    logger = redis_logger
    
    while True:
        try:
            if not global_redis_client_instance:
                await asyncio.sleep(60)
                continue
                
            async with AsyncSessionLocal() as db:
                from app.services.pending_orders import get_all_pending_orders_from_redis
                pending_orders = await get_all_pending_orders_from_redis(global_redis_client_instance)
                
                if not pending_orders:
                    await asyncio.sleep(300)
                    continue
                
                from app.crud.crud_order import get_order_model
                cleaned_count = 0
                
                for order_data in pending_orders:
                    try:
                        order_id = order_data.get('order_id')
                        user_id = order_data.get('order_user_id')
                        user_type = order_data.get('user_type', 'live')
                        symbol = order_data.get('order_company_name')
                        order_type = order_data.get('order_type')
                        
                        if not all([order_id, user_id, symbol, order_type]):
                            continue
                        
                        order_model = get_order_model(user_type)
                        
                        from app.crud.crud_order import get_order_by_id
                        db_order = await get_order_by_id(db, order_id, order_model)
                        
                        if not db_order or db_order.order_status != 'PENDING':
                            from app.services.pending_orders import remove_pending_order
                            await remove_pending_order(
                                global_redis_client_instance,
                                str(order_id),
                                symbol,
                                order_type,
                                str(user_id)
                            )
                            cleaned_count += 1
                            
                    except Exception:
                        continue
                    
        except Exception as e:
            logger.error("Error in cleanup process")
        
        await asyncio.sleep(300)