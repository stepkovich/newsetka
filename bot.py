import asyncio
import logging
import os
import time
from collections import deque
from decimal import Decimal, ROUND_DOWN

from dotenv import load_dotenv

# All imports from the same path as examples.
# binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures
# Configuration classes + DerivativesTradingUsdsFutures + PROD URLs
# (from examples in repo — they import from this deep path)
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
    ConfigurationRestAPI,
    ConfigurationWebSocketAPI,
    ConfigurationWebSocketStreams,
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL,
)
# TESTNET URLs — not in deep module, only in SDK top-level __init__.py
from binance_sdk_derivatives_trading_usds_futures import (
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_TESTNET_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_TESTNET_URL,
    ClientError,
    TooManyRequestsError,
    ServerError,
    NetworkError,
    BadRequestError,
)
# IMPORTANT: BadRequestError is NOT a subclass of ClientError! They are siblings.
# Both inherit from binance_common.errors.Error. Catching ClientError alone
# misses all HTTP 400 errors (which come as BadRequestError), including
# -5027 "No need to modify" and -2022 "ReduceOnly Order is rejected".
# We catch both via tuple: except (ClientError, BadRequestError)
# REST API DEMO URL — only in binance_common.constants, not re-exported by SDK
from binance_common.constants import (
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_DEMO_URL,
)

# NOTE: WebSocket Streams user_data() callback receives raw dict, NOT model objects.
# (from common/websocket.py: oneOf models short-circuit to raw dict)
# Event type detected by data["e"] field:
#   "ORDER_TRADE_UPDATE", "ACCOUNT_UPDATE", "listenKeyExpired", etc.
# Field names in raw dict match the model field names exactly.
# We do NOT import OrderTradeUpdate/AccountUpdate/Listenkeyexpired models
# because the callback never receives them — it gets raw dicts.

# REST API enums — kept for batch grid operations and startup config
# (from examples/rest_api/Trade/new_algo_order.py)
from binance_sdk_derivatives_trading_usds_futures.rest_api.models import (
    ChangeMarginTypeMarginTypeEnum,
    PlaceMultipleOrdersBatchOrdersParameterInner,
    NewAlgoOrderSideEnum as RestNewAlgoOrderSideEnum,
    NewAlgoOrderPositionSideEnum as RestNewAlgoOrderPositionSideEnum,
    NewAlgoOrderWorkingTypeEnum as RestNewAlgoOrderWorkingTypeEnum,
    NewOrderSideEnum as RestNewOrderSideEnum,
    NewOrderPositionSideEnum as RestNewOrderPositionSideEnum,
    ModifyOrderSideEnum as RestModifyOrderSideEnum,
)
# WS API enums — used for TP/SL placement via WebSocket API
# (from examples/websocket_api/Trade/new_order.py)
from binance_sdk_derivatives_trading_usds_futures.websocket_api.models import (
    NewOrderSideEnum,
    NewOrderPositionSideEnum,
)
# (from examples/websocket_api/Trade/modify_order.py)
from binance_sdk_derivatives_trading_usds_futures.websocket_api.models import (
    ModifyOrderSideEnum,
)
# (from examples/websocket_api/Trade/new_algo_order.py)
from binance_sdk_derivatives_trading_usds_futures.websocket_api.models import (
    NewAlgoOrderSideEnum,
    NewAlgoOrderPositionSideEnum,
    NewAlgoOrderWorkingTypeEnum,
)

from config import (
    ENVIRONMENT, SYMBOLS, SYMBOL_SETTINGS, HEDGE_MODE,
    GRID_ORDERS_PER_SIDE, GRID_BASE_STEP_PERCENT, GRID_STEP_MODE,
    GRID_VOLUME_MULTIPLIER, GRID_CANCEL_SHIFT_PERCENT,
    MIN_ORDER_USD, MAX_ORDER_USD, BALANCE_USAGE_PERCENT,
    TP_PERCENT, SL_PERCENT,
    TREND_PROTECTION, TREND_WARMUP_MODE,
    REGRESSION_FAST_WINDOW, REGRESSION_SLOW_WINDOW,
    TREND_THRESHOLD_PERCENT,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-5s | %(message)s")


def get_urls():
    """Select URLs based on ENVIRONMENT config.

    demo:    REST = DEMO URL (from binance_common.constants),
             WS API & WS Streams = TESTNET URLs (demo WS URLs not in repo,
             but user's working bot confirms testnet WS works for demo)
    testnet: all TESTNET URLs
    mainnet: all PROD URLs
    """
    if ENVIRONMENT == "demo":
        return (
            DERIVATIVES_TRADING_USDS_FUTURES_REST_API_DEMO_URL,
            DERIVATIVES_TRADING_USDS_FUTURES_WS_API_TESTNET_URL,
            DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_TESTNET_URL,
        )
    if ENVIRONMENT == "testnet":
        return (
            DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL,
            DERIVATIVES_TRADING_USDS_FUTURES_WS_API_TESTNET_URL,
            DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_TESTNET_URL,
        )
    return (
        DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
        DERIVATIVES_TRADING_USDS_FUTURES_WS_API_PROD_URL,
        DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL,
    )


def get_api_keys():
    """Get API keys based on ENVIRONMENT.

    Pattern from user's smart_scalper_bot.py:
    - demo/testnet: BINANCE_API_KEY_TESTNET / BINANCE_API_SECRET_TESTNET
    - mainnet:      BINANCE_API_KEY / BINANCE_API_SECRET
    """
    if ENVIRONMENT in ("demo", "testnet"):
        return (
            os.getenv("BINANCE_API_KEY_TESTNET", ""),
            os.getenv("BINANCE_API_SECRET_TESTNET", ""),
        )
    return (
        os.getenv("BINANCE_API_KEY", ""),
        os.getenv("BINANCE_API_SECRET", ""),
    )


def round_price(price: Decimal, tick_size: Decimal) -> str:
    """Round price to tick_size precision and return as string.

    All calculations use Decimal — no float anywhere.
    tick_size examples: "0.10" for BTCUSDT, "0.000010" for DOGEUSDT
    All batch order prices must be strings (per PlaceMultipleOrdersBatchOrdersParameterInner model).
    """
    # Round down to nearest tick_size
    rounded = (price / tick_size).to_integral_value(rounding=ROUND_DOWN) * tick_size
    # Format: use tick_size decimal places (no scientific notation, no trailing zeros issue)
    # tick_size "0.10" → 2 places, "0.000010" → 6 places
    decimal_places = max(0, -tick_size.as_tuple().exponent)
    return format(rounded, f'.{decimal_places}f')


def round_quantity(quantity: Decimal, step_size: Decimal) -> str:
    """Round quantity to step_size precision and return as string.

    step_size from LOT_SIZE filter in exchange_information.
    Examples: "0.001" for BTCUSDT, "1" for DOGEUSDT
    All batch order quantities must be strings (per PlaceMultipleOrdersBatchOrdersParameterInner model).
    Always rounds DOWN (never exceed intended position size).
    """
    # Round down to nearest step_size
    rounded = (quantity / step_size).to_integral_value(rounding=ROUND_DOWN) * step_size
    # Format: use step_size decimal places
    # step_size "0.001" → 3 places, "1" → 0 places
    decimal_places = max(0, -step_size.as_tuple().exponent)
    return format(rounded, f'.{decimal_places}f')


# =========================================================================
# Trend protection (linear regression on mark price)
# =========================================================================

class PriceBuffer:
    """Circular buffer for mark price history, trimmed by time window.

    Stores (timestamp, price) pairs where timestamp is float (seconds since epoch)
    and price is Decimal. Automatically removes data older than max_seconds
    from the latest timestamp — acts as a sliding window / ring buffer.
    """

    def __init__(self, max_seconds: int):
        self.max_seconds = max_seconds
        self._buffer = deque()
        self._ready = False

    def add(self, timestamp: float, price: Decimal):
        """Add a new price point and trim old data."""
        self._buffer.append((timestamp, price))
        cutoff = timestamp - self.max_seconds
        while self._buffer and self._buffer[0][0] < cutoff:
            self._buffer.popleft()
        if not self._ready and len(self._buffer) >= 2:
            span = self._buffer[-1][0] - self._buffer[0][0]
            if span >= self.max_seconds * 0.9:
                self._ready = True

    def is_ready(self) -> bool:
        """Whether the buffer has data spanning at least 90% of the window."""
        return self._ready

    def data(self):
        """Return all buffered data as list of (timestamp, price) tuples."""
        return list(self._buffer)

    def __len__(self):
        return len(self._buffer)


def calculate_regression_slope(buffer: PriceBuffer) -> Decimal | None:
    """Calculate linear regression slope from price buffer.

    Returns normalized slope as percentage change over the window,
    or None if not enough data.

    Uses standard linear regression formula:
        slope = (n * sum(xy) - sum(x) * sum(y)) / (n * sum(x^2) - (sum(x))^2)

    Normalized: (slope * time_span) / mean_price * 100
    This gives percentage like 0.5% or -1.2% — comparable across
    different-priced assets (BTC at 100k vs DOGE at 0.1).
    """
    points = buffer.data()
    n = len(points)
    if n < 2:
        return None

    t0 = Decimal(str(points[0][0]))

    sum_x = Decimal("0")
    sum_y = Decimal("0")
    sum_xy = Decimal("0")
    sum_x2 = Decimal("0")

    for timestamp, price in points:
        x = Decimal(str(timestamp)) - t0
        y = price
        sum_x += x
        sum_y += y
        sum_xy += x * y
        sum_x2 += x * x

    n_dec = Decimal(n)
    denominator = n_dec * sum_x2 - sum_x * sum_x

    if denominator == Decimal("0"):
        return None

    slope = (n_dec * sum_xy - sum_x * sum_y) / denominator

    time_span = Decimal(str(points[-1][0])) - t0
    if time_span <= Decimal("0"):
        return None

    mean_price = sum_y / n_dec
    if mean_price <= Decimal("0"):
        return None

    normalized = (slope * time_span) / mean_price * Decimal("100")
    return normalized


def get_trend(fast_buffer: PriceBuffer, slow_buffer: PriceBuffer,
              threshold: Decimal) -> str:
    """Determine trend direction from regression slopes.

    Uses slow regression as primary direction, fast as confirmation.

    CRITICAL: Both buffers must be "ready" (is_ready()) before any trend
    is determined. Without this check, get_trend() would return "NEUTRAL"
    with just 2 data points (~6 seconds), making TREND_WARMUP_MODE="full"
    useless — the bot would place grids almost immediately instead of
    waiting for sufficient data to calculate a reliable regression slope.

    The slow buffer requires data spanning ≥90% of REGRESSION_SLOW_WINDOW
    (~18 minutes with default 1200s) before it reports is_ready()=True.
    The fast buffer requires data spanning ≥90% of REGRESSION_FAST_WINDOW
    (~4.5 minutes with default 300s).

    Returns:
        "UP"      — uptrend, do not trade SHORT
        "DOWN"    — downtrend, do not trade LONG
        "NEUTRAL" — sideways, trade both sides
        "UNKNOWN" — not enough data yet (buffers not ready)
    """
    # Buffers must be ready before we trust any regression result.
    # Without this, 2 data points are enough for calculate_regression_slope()
    # to return a number, and get_trend() would return "NEUTRAL" almost
    # immediately — defeating the purpose of TREND_WARMUP_MODE="full".
    if not slow_buffer.is_ready() or not fast_buffer.is_ready():
        return "UNKNOWN"

    slow_slope = calculate_regression_slope(slow_buffer)
    fast_slope = calculate_regression_slope(fast_buffer)

    if slow_slope is None:
        return "UNKNOWN"

    # Slow slope determines direction, fast slope must agree (or be unknown)
    if slow_slope > threshold and (fast_slope is None or fast_slope > Decimal("0")):
        return "UP"
    if slow_slope < -threshold and (fast_slope is None or fast_slope < Decimal("0")):
        return "DOWN"

    return "NEUTRAL"


def should_trade_side(trend: str, position_side: str) -> bool:
    """Check if we should trade a given side based on current trend.

    Rules:
    - TREND_PROTECTION disabled → always True
    - Trend UNKNOWN, warmup "immediate" → True (trade until trend is known)
    - Trend UNKNOWN, warmup "full" → False (wait for data)
    - Trend UP → SHORT is blocked
    - Trend DOWN → LONG is blocked
    - Trend NEUTRAL → both sides allowed
    """
    if not TREND_PROTECTION:
        return True
    if trend == "UNKNOWN":
        return TREND_WARMUP_MODE == "immediate"
    if trend == "UP" and position_side == "SHORT":
        return False
    if trend == "DOWN" and position_side == "LONG":
        return False
    return True


# =========================================================================
# Fibonacci sequence and grid geometry helpers
# =========================================================================

def fibonacci_sequence(n: int) -> list[int]:
    """Return first n Fibonacci numbers (starting with 1, 1, 2, 3, 5, ...).

    These are used as multipliers for grid level gaps:
      Level 1 gap = fib(1) * base_step = 1 * base_step
      Level 2 gap = fib(2) * base_step = 1 * base_step
      Level 3 gap = fib(3) * base_step = 2 * base_step
      Level 4 gap = fib(4) * base_step = 3 * base_step
      ...

    The Fibonacci sequence creates a "funnel" shape — dense at the top
    (catches micro-dips), wide at the bottom (protects against deep dumps).
    """
    if n <= 0:
        return []
    if n == 1:
        return [1]
    fibs = [1, 1]
    for _ in range(n - 2):
        fibs.append(fibs[-1] + fibs[-2])
    return fibs


def grid_level_cumulative_steps(n_levels: int) -> list[Decimal]:
    """Return cumulative step multipliers for each grid level.

    For Fibonacci mode:
      Level i cumulative = sum(fib(1..i)) * base_step
      Level 1 = 1, Level 2 = 2, Level 3 = 4, Level 4 = 7, ...

    For geometric mode (old behavior):
      Level i cumulative = i * base_step
      Level 1 = 1, Level 2 = 2, Level 3 = 3, ...

    Returns list of Decimal cumulative multipliers (multiply by base_step
    to get actual percentage distance from center).
    """
    if GRID_STEP_MODE == "fibonacci":
        fibs = fibonacci_sequence(n_levels)
        cumulative = []
        running_sum = 0
        for f in fibs:
            running_sum += f
            cumulative.append(Decimal(running_sum))
        return cumulative
    else:
        # Geometric (old behavior): each level is base_step further
        return [Decimal(i) for i in range(1, n_levels + 1)]


def grid_level_volume_multipliers(n_levels: int) -> list[Decimal]:
    """Return volume multiplier for each grid level.

    qty at level i = base_qty * multiplier^(i-1)
    Returns [1, m, m^2, m^3, ...] for i=1..n_levels
    """
    m = Decimal(str(GRID_VOLUME_MULTIPLIER))
    result = []
    for i in range(n_levels):
        result.append(m ** Decimal(i))
    return result


def total_volume_multiplier(n_levels: int) -> Decimal:
    """Sum of all volume multipliers = total notional / base_notional.

    Geometric series: sum = (m^N - 1) / (m - 1)  when m != 1
    Equal volume:     sum = N                       when m == 1
    """
    m = Decimal(str(GRID_VOLUME_MULTIPLIER))
    n = Decimal(n_levels)
    if m == Decimal("1"):
        return n
    return (m ** n - Decimal("1")) / (m - Decimal("1"))


# =========================================================================
# Balance and order sizing
# =========================================================================

async def get_available_balance(ws_api_connection) -> Decimal:
    """Get USDT available balance via WebSocket API.

    (from examples/websocket_api/Account/futures_account_balance_v2.py)

    Returns Decimal(0) if balance cannot be retrieved.
    """
    try:
        response = await ws_api_connection.futures_account_balance_v2(
            recv_window=5000,
        )

        rate_limits = response.rate_limits
        logging.info(f"futures_account_balance_v2() rate limits: {rate_limits}")

        data = response.data()
        # WS API response: data.result is a list of balance items
        # (unlike REST where data() returns the list directly)
        items = data.result if data.result else []
        for item in items:
            if item.asset == "USDT" and item.available_balance is not None:
                balance = Decimal(item.available_balance)
                logging.info(f"USDT available balance: {balance}")
                return balance

        logging.warning("USDT not found in futures_account_balance_v2 response")
        return Decimal("0")

    except Exception as e:
        logging.error(f"futures_account_balance_v2() error: {e}")
        return Decimal("0")


def calculate_order_quantity(
    available_balance: Decimal,
    symbol: str,
    leverage: int,
    mark_price: Decimal,
    step_size: Decimal,
    min_qty: Decimal,
    min_notional: Decimal,
) -> tuple:
    """Calculate BASE quantity (level 1) per grid order based on available balance.

    With DCA volume (GRID_VOLUME_MULTIPLIER), later levels have larger qty:
      Level i qty = base_qty * multiplier^(i-1)

    Budget is split across all levels:
      notional_per_side = total notional for one side (LONG or SHORT)
      base_notional = notional_per_side / total_volume_multiplier(N)
      base_qty = base_notional / mark_price → round down by step_size

    Checks:
      - base_notional >= MIN_ORDER_USD (base level must be viable)
      - base_qty >= min_qty (LOT_SIZE)
      - base_qty * mark_price >= min_notional (MIN_NOTIONAL)

    Returns: (base_quantity_str, base_notional, skipped)
      base_quantity_str: "0.001" or "" if skipped — qty for LEVEL 1 only
      base_notional: Decimal (notional for level 1)
      skipped: True if order cannot be placed (below minimum)
    """
    min_order_usd = Decimal(MIN_ORDER_USD)
    max_order_usd = Decimal(MAX_ORDER_USD)
    balance_usage = Decimal(BALANCE_USAGE_PERCENT) / Decimal("100")

    # Step 1: How much of our balance we actually use
    usable_balance = available_balance * balance_usage
    logging.info(f"  {symbol}: available={available_balance} USDT, "
                 f"usage={BALANCE_USAGE_PERCENT}%, usable={usable_balance}")

    # Step 2: Equal split between symbols
    num_symbols = Decimal(len(SYMBOLS))
    margin_per_symbol = usable_balance / num_symbols
    logging.info(f"  {symbol}: margin_per_symbol={margin_per_symbol} "
                 f"(divided by {num_symbols} symbols)")

    # Step 3: Apply leverage → notional per symbol, then split by side
    notional_per_symbol = margin_per_symbol * Decimal(leverage)
    notional_per_side = notional_per_symbol / Decimal("2")

    # Step 4: Calculate base notional (level 1) from total side budget
    # total_notional = base_notional * total_volume_multiplier(N)
    # → base_notional = notional_per_side / total_volume_multiplier(N)
    vol_sum = total_volume_multiplier(GRID_ORDERS_PER_SIDE)
    base_notional = notional_per_side / vol_sum

    logging.info(f"  {symbol}: leverage={leverage}x, notional_per_symbol={notional_per_symbol}, "
                 f"notional_per_side={notional_per_side}, "
                 f"vol_sum={vol_sum}, base_notional={base_notional}")

    # Step 5: Clamp base_notional to MIN/MAX
    if base_notional > max_order_usd:
        logging.info(f"  {symbol}: base_notional {base_notional} > MAX {max_order_usd} → clamped")
        base_notional = max_order_usd

    if base_notional < min_order_usd:
        logging.warning(f"  {symbol}: base_notional {base_notional} < MIN {min_order_usd} → "
                        f"CANNOT PLACE ORDER (below minimum)")
        return ("", base_notional, True)

    # Step 6: Convert to quantity and round
    quantity = base_notional / mark_price
    quantity_str = round_quantity(quantity, step_size)
    actual_quantity = Decimal(quantity_str)

    # Step 7: Check LOT_SIZE min_qty
    if actual_quantity < min_qty:
        logging.warning(f"  {symbol}: quantity {quantity_str} < min_qty {min_qty} → "
                        f"CANNOT PLACE ORDER (below LOT_SIZE minimum)")
        return ("", base_notional, True)

    # Step 8: Check MIN_NOTIONAL
    actual_notional = actual_quantity * mark_price
    if actual_notional < min_notional:
        logging.warning(f"  {symbol}: actual_notional {actual_notional} < min_notional {min_notional} → "
                        f"CANNOT PLACE ORDER (below MIN_NOTIONAL)")
        return ("", base_notional, True)

    # Log the full grid budget breakdown
    vol_mults = grid_level_volume_multipliers(GRID_ORDERS_PER_SIDE)
    last_notional = base_notional * vol_mults[-1]
    total_notional = base_notional * vol_sum
    logging.info(f"  {symbol}: BASE quantity={quantity_str}, "
                 f"base_notional={actual_notional} "
                 f"(my margin={actual_notional / Decimal(leverage)})")
    logging.info(f"  {symbol}: GRID budget: base={actual_notional}, "
                 f"last_level={last_notional}, "
                 f"total_side={total_notional}, "
                 f"multiplier={GRID_VOLUME_MULTIPLIER}x over {GRID_ORDERS_PER_SIDE} levels")

    return (quantity_str, base_notional, False)


# =========================================================================
# Grid management functions
# =========================================================================

def build_grid_orders(symbol: str, mark_price: Decimal, tick_size: Decimal,
                      base_qty_str: str, step_size: Decimal,
                      position_side: str):
    """Build grid orders for one side (LONG or SHORT).

    (from examples/rest_api/Trade/place_multiple_orders.py —
     PlaceMultipleOrdersBatchOrdersParameterInner fields are all strings)

    Fibonacci + DCA grid:
    - Step: level i gap = fib(i) * base_step, cumulative from center
      LONG:  price_i = center * (1 - cum_step_i * base_step)
      SHORT: price_i = center * (1 + cum_step_i * base_step)
    - Volume: level i qty = base_qty * multiplier^(i-1)
      Each level has a DIFFERENT quantity (DCA scaling).

    Order type: LIMIT with time_in_force=GTX (Good Till Crossing).
    GTX guarantees MAKER-only execution — if the order would cross the spread
    (fill immediately as taker), it is cancelled instead. This is critical
    for the grid strategy: we never want to pay taker fees.

    Args:
        symbol: trading pair
        mark_price: current mark price (used as grid center)
        tick_size: price precision
        base_qty_str: base quantity string for level 1
        step_size: lot size precision for rounding quantities
        position_side: "LONG" or "SHORT"

    Returns:
        List of PlaceMultipleOrdersBatchOrdersParameterInner
    """
    base_step = Decimal(GRID_BASE_STEP_PERCENT) / Decimal("100")
    cum_steps = grid_level_cumulative_steps(GRID_ORDERS_PER_SIDE)
    vol_mults = grid_level_volume_multipliers(GRID_ORDERS_PER_SIDE)
    base_qty = Decimal(base_qty_str)

    if position_side == "LONG":
        order_side = "BUY"
    else:  # SHORT
        order_side = "SELL"

    orders = []
    for i in range(GRID_ORDERS_PER_SIDE):
        level = i + 1  # 1-based level number

        # Price: cumulative step distance from center
        cum_pct = cum_steps[i] * base_step
        if position_side == "LONG":
            price = mark_price * (Decimal("1") - cum_pct)
        else:  # SHORT
            price = mark_price * (Decimal("1") + cum_pct)

        price_str = round_price(price, tick_size)

        # GTX cross-spread check
        price_dec = Decimal(price_str)
        if position_side == "LONG" and price_dec >= mark_price:
            logging.info(f"  {position_side} #{level}: SKIP {price_str} >= mark {mark_price} "
                         f"(would cross spread)")
            continue
        if position_side == "SHORT" and price_dec <= mark_price:
            logging.info(f"  {position_side} #{level}: SKIP {price_str} <= mark {mark_price} "
                         f"(would cross spread)")
            continue

        # Quantity: DCA scaling
        qty = base_qty * vol_mults[i]
        qty_str = round_quantity(qty, step_size)

        # Verify minimum notional for this level
        qty_dec = Decimal(qty_str)
        if qty_dec * price_dec < Decimal("5"):
            logging.info(f"  {position_side} #{level}: SKIP notional below $5 "
                         f"(qty={qty_str} price={price_str})")
            continue

        order = PlaceMultipleOrdersBatchOrdersParameterInner(
            symbol=symbol,
            side=order_side,
            position_side=position_side,  # Hedge Mode
            type="LIMIT",
            time_in_force="GTX",
            quantity=qty_str,
            price=price_str,
            new_order_resp_type="ACK",
        )
        orders.append(order)
        logging.info(f"  {position_side} #{level}: {order_side} LIMIT GTX @ {price_str} × {qty_str} "
                     f"(gap={cum_pct * Decimal('100'):.4f}%, vol={vol_mults[i]:.2f}x)")

    return orders


def recover_center_price(entry_price: Decimal, n_filled: int,
                         position_side: str) -> Decimal:
    """Recover the original grid center_price from position entry_price.

    When the bot restarts with an existing position, we need to know the
    center_price that was used to build the grid, so we can calculate which
    levels are still open and rebuild only the missing ones.

    With Fibonacci step + DCA volume, the weighted average entry is:

      entry_price = sum(qty_i * price_i) / sum(qty_i)

    where:
      qty_i = base_qty * m^(i-1)    (DCA volume)
      price_i = center * (1 ± cum_step_i * base_step)  (Fibonacci step)

    For LONG:
      entry = center * sum(qty_i * (1 - cum_step_i * base_step)) / sum(qty_i)
      center = entry / (sum(qty_i * (1 - cum_step_i * base_step)) / sum(qty_i))

    We compute this numerically (not analytically) because Fibonacci cumulative
    steps don't have a simple closed-form expression.

    Args:
        entry_price: average entry price from exchange (position_information_v2)
        n_filled: number of filled grid levels
        position_side: "LONG" or "SHORT"

    Returns:
        Recovered center_price as Decimal.
    """
    if n_filled <= 0:
        return entry_price  # fallback

    base_step = Decimal(GRID_BASE_STEP_PERCENT) / Decimal("100")
    cum_steps = grid_level_cumulative_steps(n_filled)
    vol_mults = grid_level_volume_multipliers(n_filled)

    # Calculate weighted average price multiplier
    # weighted_avg_pct = sum(qty_i * cum_step_i * base_step) / sum(qty_i)
    numerator = Decimal("0")
    denominator = Decimal("0")
    for i in range(n_filled):
        qty_i = vol_mults[i]  # relative to base_qty
        cum_pct_i = cum_steps[i] * base_step
        if position_side == "LONG":
            price_mult_i = Decimal("1") - cum_pct_i
        else:
            price_mult_i = Decimal("1") + cum_pct_i
        numerator += qty_i * price_mult_i
        denominator += qty_i

    if denominator == Decimal("0"):
        return entry_price  # fallback

    weighted_avg_mult = numerator / denominator
    if weighted_avg_mult == Decimal("0"):
        return entry_price  # fallback

    center = entry_price / weighted_avg_mult
    return center


def build_remaining_grid_orders(symbol: str, center_price: Decimal,
                                mark_price: Decimal, tick_size: Decimal,
                                base_qty_str: str, step_size: Decimal,
                                position_side: str, n_filled: int):
    """Build grid orders for remaining (unfilled) levels only.

    Similar to build_grid_orders but starts from level (n_filled + 1)
    instead of level 1. Used when the bot restarts with an existing
    position and needs to rebuild only the missing grid levels.

    Args:
        symbol: trading pair
        center_price: original grid center price (recovered or known)
        mark_price: current mark price (for GTX cross-spread check)
        tick_size: price precision
        base_qty_str: base quantity string for level 1
        step_size: lot size precision for rounding quantities
        position_side: "LONG" or "SHORT"
        n_filled: number of already-filled grid levels

    Returns:
        List of PlaceMultipleOrdersBatchOrdersParameterInner for unfilled levels.
    """
    base_step = Decimal(GRID_BASE_STEP_PERCENT) / Decimal("100")
    cum_steps = grid_level_cumulative_steps(GRID_ORDERS_PER_SIDE)
    vol_mults = grid_level_volume_multipliers(GRID_ORDERS_PER_SIDE)
    base_qty = Decimal(base_qty_str)

    if position_side == "LONG":
        order_side = "BUY"
    else:  # SHORT
        order_side = "SELL"

    orders = []
    for i in range(n_filled, GRID_ORDERS_PER_SIDE):
        level = i + 1  # 1-based level number

        # Price: cumulative step distance from center
        cum_pct = cum_steps[i] * base_step
        if position_side == "LONG":
            price = center_price * (Decimal("1") - cum_pct)
        else:  # SHORT
            price = center_price * (Decimal("1") + cum_pct)

        price_str = round_price(price, tick_size)

        # GTX cross-spread check
        price_dec = Decimal(price_str)
        if position_side == "LONG" and price_dec >= mark_price:
            logging.info(f"  {position_side} #{level}: SKIP {price_str} >= mark {mark_price} "
                         f"(would cross spread)")
            continue
        if position_side == "SHORT" and price_dec <= mark_price:
            logging.info(f"  {position_side} #{level}: SKIP {price_str} <= mark {mark_price} "
                         f"(would cross spread)")
            continue

        # Quantity: DCA scaling
        qty = base_qty * vol_mults[i]
        qty_str = round_quantity(qty, step_size)

        # Verify minimum notional for this level
        qty_dec = Decimal(qty_str)
        if qty_dec * price_dec < Decimal("5"):
            logging.info(f"  {position_side} #{level}: SKIP notional below $5 "
                         f"(qty={qty_str} price={price_str})")
            continue

        order = PlaceMultipleOrdersBatchOrdersParameterInner(
            symbol=symbol,
            side=order_side,
            position_side=position_side,
            type="LIMIT",
            time_in_force="GTX",
            quantity=qty_str,
            price=price_str,
            new_order_resp_type="ACK",
        )
        orders.append(order)
        logging.info(f"  {position_side} #{level}: {order_side} LIMIT GTX @ {price_str} × {qty_str} "
                     f"(gap={cum_pct * Decimal('100'):.4f}%, vol={vol_mults[i]:.2f}x)")

    return orders


def place_orders_batched(client, symbol: str, orders: list) -> int:
    """Place orders in batches of 5 (API limit per request).

    (from examples/rest_api/Trade/place_multiple_orders.py)

    Returns the number of successfully placed orders.
    """
    total_orders = len(orders)
    batch_size = 5
    placed = 0
    rejected = 0

    logging.info(f"Placing {total_orders} orders for {symbol} in batches of {batch_size}")

    for batch_start in range(0, total_orders, batch_size):
        batch = orders[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (total_orders + batch_size - 1) // batch_size

        logging.info(f"Batch {batch_num}/{total_batches}: {len(batch)} orders")

        try:
            # (from examples/rest_api/Trade/place_multiple_orders.py)
            response = client.rest_api.place_multiple_orders(
                batch_orders=batch,
            )

            rate_limits = response.rate_limits
            logging.info(f"place_multiple_orders() rate limits: {rate_limits}")

            data = response.data()
            # data is iterable (PlaceMultipleOrdersResponseInner items)
            # Fields per item: order_id, client_order_id, symbol, side,
            # position_side, type, status, price, orig_qty, etc.
            # Also: code, msg for individual order errors
            for item in data:
                if item.code is not None and item.code != 200:
                    rejected += 1
                    # GTX orders are rejected if they would cross the spread
                    # (fill as taker) — this is EXPECTED behavior, not a bug
                    logging.info(f"  Order rejected (GTX post-only): code={item.code} "
                                 f"msg={item.msg} symbol={item.symbol} side={item.side} "
                                 f"price={item.price} — would cross spread")
                else:
                    placed += 1
                    logging.info(f"  Order placed: orderId={item.order_id} "
                                 f"symbol={item.symbol} side={item.side} "
                                 f"positionSide={item.position_side} "
                                 f"price={item.price} qty={item.orig_qty} "
                                 f"status={item.status}")

        except (ClientError, BadRequestError) as e:
            logging.error(f"place_multiple_orders() batch {batch_num} error: {e}")
        except Exception as e:
            logging.error(f"place_multiple_orders() batch {batch_num} error: {e}")

    # Warn if some orders were rejected — incomplete grid may need attention
    if rejected > 0:
        logging.warning(
            f"[GRID] {symbol}: {placed}/{total_orders} orders placed, "
            f"{rejected} rejected (GTX post-only) — grid is INCOMPLETE"
        )

    return placed


async def cancel_grid_side(client, symbol: str, position_side: str):
    """Cancel all open GRID orders for one side (LONG or SHORT) of a symbol.

    Two-step process (no single API call filters by side):
    1. current_all_open_orders(symbol) — get all open orders
       (from examples/rest_api/Trade/current_all_open_orders.py)
    2. Filter by position_side AND side (to preserve TP orders), collect order_ids
    3. cancel_multiple_orders(symbol, order_id_list) — cancel in batches of 10
       (from examples/rest_api/Trade/cancel_multiple_orders.py)

    IMPORTANT: We filter by BOTH position_side AND side to avoid cancelling
    TP (take-profit) orders. Grid orders and TP orders share the same
    position_side but have opposite side:
      LONG grid: side=BUY,  positionSide=LONG  → cancel these
      LONG TP:   side=SELL, positionSide=LONG  → preserve this
      SHORT grid: side=SELL, positionSide=SHORT → cancel these
      SHORT TP:  side=BUY,  positionSide=SHORT → preserve this
    """
    # Step 1: Get all open orders for the symbol
    # (from examples/rest_api/Trade/current_all_open_orders.py)
    # Note: recv_window=5000 needed for testnet stability (returns -1000 without it)
    data = None
    for attempt in range(3):
        try:
            response = client.rest_api.current_all_open_orders(
                symbol=symbol,
                recv_window=5000,
            )

            rate_limits = response.rate_limits
            logging.info(f"current_all_open_orders({symbol}) rate limits: {rate_limits}")

            data = response.data()
            break

        except Exception as e:
            logging.warning(f"current_all_open_orders({symbol}) attempt {attempt+1} error: {e}")
            if attempt < 2:
                await asyncio.sleep(2)

    if data is None:
        logging.error(f"current_all_open_orders({symbol}) failed after 3 attempts")
        return 0

    # Step 2: Filter by position_side AND side to preserve TP orders
    # Grid direction: LONG grid = BUY, SHORT grid = SELL
    # TP direction:   LONG TP = SELL, SHORT TP = BUY
    grid_side = "BUY" if position_side == "LONG" else "SELL"

    order_ids = []
    for item in data:
        if item.position_side == position_side and item.status != "CANCELED":
            # Only cancel grid orders (same side as grid direction)
            # Skip TP orders (opposite side)
            if item.side != grid_side:
                logging.info(f"  Skipping TP/SL order: orderId={item.order_id} "
                             f"side={item.side} price={item.price} (not a grid order)")
                continue
            # Skip PARTIALLY_FILLED orders — they have remaining qty
            # that should stay open to complete the fill
            if item.status == "PARTIALLY_FILLED":
                logging.info(f"  Skipping partially filled order: orderId={item.order_id} "
                             f"side={item.side} price={item.price} status={item.status}")
                continue
            order_ids.append(item.order_id)
            logging.info(f"  Found {position_side} grid order: orderId={item.order_id} "
                         f"side={item.side} price={item.price} status={item.status}")

    if not order_ids:
        logging.info(f"No open {position_side} orders for {symbol}")
        return 0

    logging.info(f"Found {len(order_ids)} {position_side} orders to cancel for {symbol}")

    # Step 3: Cancel in batches of 10 (API limit: max 10 orderIds per request)
    # (from examples/rest_api/Trade/cancel_multiple_orders.py)
    batch_size = 10
    cancelled = 0

    for batch_start in range(0, len(order_ids), batch_size):
        batch_ids = order_ids[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(order_ids) + batch_size - 1) // batch_size

        logging.info(f"Cancel batch {batch_num}/{total_batches}: {len(batch_ids)} orders")

        try:
            # (from examples/rest_api/Trade/cancel_multiple_orders.py)
            response = client.rest_api.cancel_multiple_orders(
                symbol=symbol,
                order_id_list=batch_ids,
            )

            rate_limits = response.rate_limits
            logging.info(f"cancel_multiple_orders() rate limits: {rate_limits}")

            data = response.data()
            for item in data:
                if hasattr(item, 'code') and item.code is not None and item.code != 200:
                    logging.warning(f"  Cancel error: code={item.code} msg={item.msg} "
                                    f"orderId={item.order_id}")
                else:
                    logging.info(f"  Cancelled: orderId={item.order_id} "
                                 f"symbol={item.symbol} side={item.side} "
                                 f"positionSide={item.position_side} "
                                 f"price={item.price} status={item.status}")
                    cancelled += 1

        except (ClientError, BadRequestError) as e:
            logging.error(f"cancel_multiple_orders() batch {batch_num} error: {e}")
        except Exception as e:
            logging.error(f"cancel_multiple_orders() batch {batch_num} error: {e}")

    return cancelled


# =========================================================================
# Position info, Take-profit and Stop-loss
# =========================================================================

async def get_positions_for_symbol(ws_api_connection, symbol: str) -> dict:
    """Get ALL positions for a symbol via WebSocket API (one call for both sides).

    (from examples/websocket_api/Trade/position_information_v2.py)

    position_information_v2(symbol=) returns ALL positions for the symbol —
    both LONG and SHORT in hedge mode. There is no positionSide parameter
    on the API, so we get both sides in one call and return a dict keyed
    by position_side.

    Returns:
        dict: {
            "LONG": {"entry_price": Decimal, "position_amt": Decimal} or None,
            "SHORT": {"entry_price": Decimal, "position_amt": Decimal} or None,
        }
        Each side is None if no position exists for that side.
    """
    result = {"LONG": None, "SHORT": None}

    try:
        # (from examples/websocket_api/Trade/position_information_v2.py)
        response = await ws_api_connection.position_information_v2(
            symbol=symbol,
            recv_window=5000,
        )

        data = response.data()
        # WS API response: data.result is a list of position items
        items = data.result if data.result else []
        for item in items:
            if item.symbol == symbol and item.position_side in ("LONG", "SHORT"):
                entry_price_str = item.entry_price
                position_amt_str = item.position_amt
                if entry_price_str and position_amt_str:
                    entry_price = Decimal(entry_price_str)
                    position_amt = Decimal(position_amt_str)
                    if position_amt != Decimal("0"):
                        ps = item.position_side
                        logging.info(f"Position {symbol} {ps}: "
                                     f"entry_price={entry_price} position_amt={position_amt}")
                        result[ps] = {
                            "entry_price": entry_price,
                            "position_amt": position_amt,
                        }

    except Exception as e:
        logging.error(f"position_information_v2({symbol}) error: {e}")

    return result


def compute_sl_trigger(symbol: str, mark_price: Decimal, tick_size: Decimal,
                       position_side: str) -> str:
    """Compute stop-loss trigger price: last grid level ± SL_PERCENT.

    Last grid level is at cumulative Fibonacci/geometric step distance
    from the center (mark_price):
    - LONG: last_level = mark_price * (1 - cum_step_N * base_step)
            SL trigger = last_level * (1 - SL_PERCENT/100)
    - SHORT: last_level = mark_price * (1 + cum_step_N * base_step)
             SL trigger = last_level * (1 + SL_PERCENT/100)

    Returns trigger price as string (rounded by tick_size).
    """
    sl_percent = Decimal(SL_PERCENT) / Decimal("100")
    base_step = Decimal(GRID_BASE_STEP_PERCENT) / Decimal("100")
    cum_steps = grid_level_cumulative_steps(GRID_ORDERS_PER_SIDE)
    last_cum_pct = cum_steps[-1] * base_step  # deepest level distance

    if position_side == "LONG":
        last_level = mark_price * (Decimal("1") - last_cum_pct)
        trigger = last_level * (Decimal("1") - sl_percent)
    else:  # SHORT
        last_level = mark_price * (Decimal("1") + last_cum_pct)
        trigger = last_level * (Decimal("1") + sl_percent)

    trigger_str = round_price(trigger, tick_size)
    logging.info(f"SL trigger for {symbol} {position_side}: "
                 f"last_level={round_price(last_level, tick_size)}, "
                 f"trigger={trigger_str} ({SL_PERCENT}% from last level, "
                 f"coverage={last_cum_pct * Decimal('100'):.2f}%)")
    return trigger_str


async def place_stop_loss(ws_api_connection, symbol: str, position_side: str,
                          trigger_price: str) -> int:
    """Place a STOP_MARKET close-position order via WebSocket API.

    (from examples/websocket_api/Trade/new_algo_order.py)

    Uses new_algo_order because new_order does NOT have
    stop_price/close_position/working_type parameters.

    Key params:
    - algo_type="CONDITIONAL" (required for TP/SL orders)
    - type="STOP_MARKET" (market order on trigger)
    - close_position="true" (closes ENTIRE position, no quantity needed)
    - trigger_price=string (the trigger price)
    - working_type="MARK_PRICE" (trigger by mark price, more reliable)
    - price_protect="true" (protect against false triggers)

    In hedge mode:
    - LONG SL: side=SELL, position_side=LONG
    - SHORT SL: side=BUY, position_side=SHORT

    Returns algoId if successful, 0 if failed.
    """
    if position_side == "LONG":
        side = NewAlgoOrderSideEnum["SELL"].value
        ps = NewAlgoOrderPositionSideEnum["LONG"].value
    else:  # SHORT
        side = NewAlgoOrderSideEnum["BUY"].value
        ps = NewAlgoOrderPositionSideEnum["SHORT"].value

    logging.info(f"Placing STOP_MARKET for {symbol} {position_side}: "
                 f"side={side} trigger={trigger_price} closePosition=true")

    try:
        # (from examples/websocket_api/Trade/new_algo_order.py)
        response = await ws_api_connection.new_algo_order(
            algo_type="CONDITIONAL",
            symbol=symbol,
            side=side,
            type="STOP_MARKET",
            position_side=ps,
            trigger_price=float(trigger_price),
            close_position="true",
            working_type=NewAlgoOrderWorkingTypeEnum["MARK_PRICE"].value,
            price_protect="true",
            new_order_resp_type="ACK",
            recv_window=5000,
        )

        data = response.data()
        # WS API: data.result contains the actual response
        result = data.result
        algo_id = result.algo_id if result and result.algo_id else 0
        logging.info(f"STOP_MARKET placed: algoId={algo_id} symbol={symbol} "
                     f"positionSide={position_side} trigger={trigger_price}")
        return algo_id

    except (ClientError, BadRequestError) as e:
        logging.error(f"new_algo_order STOP_MARKET error: {e}")
        return 0
    except Exception as e:
        logging.error(f"new_algo_order STOP_MARKET error: {e}")
        return 0


async def place_take_profit(ws_api_connection, symbol: str, position_side: str,
                            entry_price: Decimal, position_amt: Decimal,
                            tick_size: Decimal, step_size: Decimal) -> int:
    """Place a LIMIT GTC take-profit order via WebSocket API.

    (from examples/websocket_api/Trade/new_order.py)

    Returns orderId if successful, 0 if failed.
    """
    tp_percent = Decimal(TP_PERCENT) / Decimal("100")

    if position_side == "LONG":
        tp_price = entry_price * (Decimal("1") + tp_percent)
        side = NewOrderSideEnum["SELL"].value
        ps = NewOrderPositionSideEnum["LONG"].value
    else:  # SHORT
        tp_price = entry_price * (Decimal("1") - tp_percent)
        side = NewOrderSideEnum["BUY"].value
        ps = NewOrderPositionSideEnum["SHORT"].value

    tp_price_str = round_price(tp_price, tick_size)
    tp_qty = abs(position_amt)
    tp_qty_str = round_quantity(tp_qty, step_size)

    logging.info(f"Placing TAKE-PROFIT for {symbol} {position_side}: "
                 f"side={side} price={tp_price_str} qty={tp_qty_str} "
                 f"(entry={entry_price}, TP={TP_PERCENT}%)")

    try:
        # (from examples/websocket_api/Trade/new_order.py)
        response = await ws_api_connection.new_order(
            symbol=symbol,
            side=side,
            type="LIMIT",
            position_side=ps,
            time_in_force="GTC",
            quantity=float(tp_qty_str),
            price=float(tp_price_str),
            new_order_resp_type="ACK",
            recv_window=5000,
        )

        data = response.data()
        # WS API: data.result contains the actual response
        result = data.result
        order_id = result.order_id if result and result.order_id else 0
        logging.info(f"TAKE-PROFIT placed: orderId={order_id} symbol={symbol} "
                     f"positionSide={position_side} price={tp_price_str} qty={tp_qty_str}")
        return order_id

    except (ClientError, BadRequestError) as e:
        logging.error(f"new_order TAKE-PROFIT error: {e}")
        return 0
    except Exception as e:
        logging.error(f"new_order TAKE-PROFIT error: {e}")
        return 0


async def modify_take_profit(ws_api_connection, symbol: str, position_side: str,
                             tp_order_id: int, entry_price: Decimal,
                             position_amt: Decimal,
                             tick_size: Decimal, step_size: Decimal) -> bool:
    """Modify existing TP order via WebSocket API: update price and quantity.

    (from examples/websocket_api/Trade/modify_order.py)

    modify_order requires both quantity and price (both mandatory).
    Cannot modify stop_price — but our SL is close_position=true so no need.

    In hedge mode:
    - LONG TP: side=SELL
    - SHORT TP: side=BUY

    Returns True if successful, False if failed.
    """
    tp_percent = Decimal(TP_PERCENT) / Decimal("100")

    if position_side == "LONG":
        tp_price = entry_price * (Decimal("1") + tp_percent)
        side = ModifyOrderSideEnum["SELL"].value
    else:  # SHORT
        tp_price = entry_price * (Decimal("1") - tp_percent)
        side = ModifyOrderSideEnum["BUY"].value

    tp_price_str = round_price(tp_price, tick_size)
    tp_qty = abs(position_amt)
    tp_qty_str = round_quantity(tp_qty, step_size)

    logging.info(f"Modifying TAKE-PROFIT orderId={tp_order_id} for {symbol} {position_side}: "
                 f"new_price={tp_price_str} new_qty={tp_qty_str}")

    try:
        # (from examples/websocket_api/Trade/modify_order.py)
        response = await ws_api_connection.modify_order(
            symbol=symbol,
            side=side,
            quantity=float(tp_qty_str),
            price=float(tp_price_str),
            order_id=tp_order_id,
            recv_window=5000,
        )

        logging.info(f"TAKE-PROFIT modified: orderId={tp_order_id} symbol={symbol} "
                     f"price={tp_price_str} qty={tp_qty_str}")
        return True

    except (ClientError, BadRequestError) as e:
        # Error -5027: "No need to modify the order" means TP already has
        # the correct price and quantity — treat as SUCCESS, not failure.
        # WS API may throw BadRequestError or ValueError for -5027.
        # Check string representation as fallback for all error types.
        is_no_modify = (
            (hasattr(e, 'status_code') and e.status_code == -5027)
            or "-5027" in str(e)
            or (hasattr(e, 'error_message') and e.error_message
                and "No need to modify" in str(e.error_message))
        )
        if is_no_modify:
            logging.info(f"modify_order TAKE-PROFIT: order already correct "
                         f"orderId={tp_order_id} (-5027 No need to modify)")
            return True
        logging.error(f"modify_order TAKE-PROFIT error: {e}")
        return False
    except Exception as e:
        # Catch-all: also check for -5027 in string representation
        # (WS API may throw ValueError instead of ClientError)
        if "-5027" in str(e) or "No need to modify" in str(e):
            logging.info(f"modify_order TAKE-PROFIT: order already correct "
                         f"orderId={tp_order_id} (-5027 in catch-all)")
            return True
        logging.error(f"modify_order TAKE-PROFIT error: {e}")
        return False


async def cancel_tp_order(ws_api_connection, symbol: str, order_id: int) -> bool:
    """Cancel a take-profit order via WebSocket API.

    (from examples/websocket_api/Trade/cancel_order.py)

    Returns True if successful, False if failed.
    """
    try:
        response = await ws_api_connection.cancel_order(
            symbol=symbol,
            order_id=order_id,
            recv_window=5000,
        )
        logging.info(f"TP cancelled: orderId={order_id} symbol={symbol}")
        return True
    except Exception as e:
        # -2011: "Unknown order sent" means already cancelled/filled
        if "-2011" in str(e) or "Unknown order" in str(e):
            logging.info(f"TP already cancelled/filled: orderId={order_id}")
            return True
        logging.warning(f"cancel_order error for orderId={order_id}: {e}")
        return False


# =========================================================================
# API Error Handling with Retry (Feature 4)
# =========================================================================

async def retry_api_call(coro_func, max_retries=3, base_delay=1.0,
                         operation_name="API call", verify_already_placed=None):
    """Retry an async API call with exponential backoff.

    For POST operations (place order), before retrying:
    1. If the operation was 'place TP/SL', check if order already exists
       via REST (open orders) — to avoid duplicate orders
    2. If order already placed, don't retry

    The place_take_profit and place_stop_loss functions already handle their
    own exception logging internally and return 0 on failure or a non-zero
    ID on success. This wrapper checks the return value and retries on
    failure (0 or None). For direct API calls that may throw exceptions,
    those are caught and handled here.

    Args:
        coro_func: async callable (no args) — the API call to make
        max_retries: maximum number of attempts
        base_delay: base delay in seconds (doubles each retry)
        operation_name: human-readable name for logging
        verify_already_placed: optional async callable that returns True if
            the operation was already completed (to avoid duplicate orders)
    """
    for attempt in range(max_retries):
        try:
            result = await coro_func()

            if result:  # Success (non-zero ID, or truthy value)
                return result

            # Result is 0 or None — failure
            if attempt < max_retries - 1:
                # Before retrying, check if already placed
                if verify_already_placed:
                    try:
                        already_placed = await verify_already_placed()
                        if already_placed:
                            logging.info(
                                f"[RETRY] {operation_name}: order already exists "
                                f"on exchange, skipping retry"
                            )
                            return 0  # Don't retry, but return 0 to indicate no new order
                    except Exception as e:
                        logging.warning(
                            f"[RETRY] {operation_name}: verify check failed: {e}"
                        )

                delay = base_delay * (2 ** attempt)
                logging.warning(
                    f"[RETRY] {operation_name}: attempt {attempt + 1}/{max_retries} "
                    f"failed (result={result}), retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
            else:
                logging.error(
                    f"[RETRY] {operation_name}: all {max_retries} attempts failed"
                )
                return result  # Return the last failure result (0 or None)

        except TooManyRequestsError as e:
            if attempt < max_retries - 1:
                retry_after = base_delay * 4
                logging.warning(
                    f"[RETRY] {operation_name}: rate limited (429), "
                    f"waiting {retry_after:.1f}s... ({e})"
                )
                await asyncio.sleep(retry_after)
            else:
                logging.error(
                    f"[RETRY] {operation_name}: rate limited, all attempts exhausted"
                )
                return 0

        except (ServerError, NetworkError) as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logging.warning(
                    f"[RETRY] {operation_name}: {type(e).__name__}: {e}, "
                    f"retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
            else:
                logging.error(
                    f"[RETRY] {operation_name}: {type(e).__name__}, "
                    f"all attempts exhausted"
                )
                return 0

        except (BadRequestError, ClientError) as e:
            # Permanent errors — don't retry
            logging.error(
                f"[RETRY] {operation_name}: permanent error, not retrying: {e}"
            )
            return 0

        except Exception as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logging.warning(
                    f"[RETRY] {operation_name}: unexpected error: {e}, "
                    f"retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
            else:
                logging.error(
                    f"[RETRY] {operation_name}: unexpected error, "
                    f"all attempts exhausted: {e}"
                )
                return 0

    return 0


def _make_verify_tp_exists(client, symbol: str, position_side: str):
    """Create a verification function that checks if a TP order already exists.

    Uses current_all_open_orders to find a LIMIT GTC order on the opposite
    side of the position (LONG position → TP is SELL, SHORT position → TP is BUY).
    """
    async def verify():
        try:
            response = client.rest_api.current_all_open_orders(
                symbol=symbol, recv_window=5000,
            )
            data = response.data()
            for item in data:
                if (item.position_side == position_side
                        and item.type == "LIMIT"
                        and item.time_in_force == "GTC"
                        and item.status not in ("CANCELED", "EXPIRED", "EXPIRED_IN_MATCH")):
                    if ((position_side == "LONG" and item.side == "SELL")
                            or (position_side == "SHORT" and item.side == "BUY")):
                        logging.info(
                            f"[VERIFY] TP already exists: orderId={item.order_id} "
                            f"{symbol} {position_side} price={item.price} qty={item.orig_qty}"
                        )
                        return True
            return False
        except Exception as e:
            logging.warning(f"[VERIFY] current_all_open_orders({symbol}) error: {e}")
            return False
    return verify


def _make_verify_sl_exists(client, symbol: str, position_side: str):
    """Create a verification function that checks if an SL order already exists.

    Uses current_all_algo_open_orders to find a STOP_MARKET algo order on the
    opposite side of the position.
    """
    async def verify():
        try:
            response = client.rest_api.current_all_algo_open_orders(
                symbol=symbol, recv_window=5000,
            )
            data = response.data()
            for item in data:
                if (item.symbol == symbol
                        and item.position_side == position_side
                        and item.order_type == "STOP_MARKET"):
                    logging.info(
                        f"[VERIFY] SL already exists: algoId={item.algo_id} "
                        f"{symbol} {position_side} triggerPrice={item.trigger_price}"
                    )
                    return True
            return False
        except Exception as e:
            logging.warning(f"[VERIFY] current_all_algo_open_orders({symbol}) error: {e}")
            return False
    return verify


# =========================================================================
# Startup State Synchronization (Feature 2)
# =========================================================================

async def sync_state_with_exchange(client, ws_api_connection, tp_sl_tracking: dict,
                                   symbol_filters: dict,
                                   position_cache: dict = None):
    """Verify that every open position has TP and SL orders.

    The exchange is the source of truth. On startup or reconnection,
    this function checks all positions and ensures TP/SL are in place.

    Optimized: one API call per symbol for each of:
    - position_information_v2 (returns ALL positions for symbol — both LONG and SHORT)
    - current_all_open_orders (returns ALL orders for symbol)
    - current_all_algo_open_orders (returns ALL algo orders for symbol)

    Then iterates over position sides in-memory — no duplicate API calls.
    """
    tp_repaired = 0
    sl_repaired = 0
    positions_checked = 0

    logging.info("[SYNC] Starting state synchronization with exchange...")

    for symbol in SYMBOLS:
        if symbol not in symbol_filters:
            continue

        tick_size = symbol_filters[symbol]["tick_size"]
        step_size = symbol_filters[symbol]["step_size"]

        # 1. Get ALL positions for this symbol (ONE WS API call for both sides)
        positions = await get_positions_for_symbol(ws_api_connection, symbol)

        # 2. Get ALL open orders for this symbol (ONE REST call)
        # (from examples/rest_api/Trade/current_all_open_orders.py)
        open_orders = []
        try:
            response = client.rest_api.current_all_open_orders(
                symbol=symbol, recv_window=5000,
            )
            open_orders = response.data()
        except Exception as e:
            logging.error(f"[SYNC] current_all_open_orders({symbol}) error: {e}")
            continue

        # 3. Get ALL open algo orders for this symbol (ONE REST call)
        # (from examples/rest_api/Trade/current_all_algo_open_orders.py)
        algo_orders = []
        try:
            response = client.rest_api.current_all_algo_open_orders(
                symbol=symbol, recv_window=5000,
            )
            algo_orders = response.data()
        except Exception as e:
            logging.error(f"[SYNC] current_all_algo_open_orders({symbol}) error: {e}")
            continue

        # 4. Now iterate over position sides in-memory (no more API calls)
        for position_side in ("LONG", "SHORT"):
            key = (symbol, position_side)
            pos = positions.get(position_side)

            if pos is None:
                # No position — clear any stale tracking
                if key in tp_sl_tracking:
                    logging.info(f"[SYNC] {symbol} {position_side}: no position, "
                                 f"clearing stale tracking")
                    del tp_sl_tracking[key]
                continue

            positions_checked += 1
            entry_price = pos["entry_price"]
            position_amt = pos["position_amt"]

            # Cache position for grid shift checks (avoid REST calls every second)
            if position_cache is not None:
                position_cache[key] = {
                    "entry_price": entry_price,
                    "position_amt": position_amt,
                }

            # 5. Find existing TP order (LIMIT GTC, opposite side of position)
            tp_order = None
            for item in open_orders:
                if (item.position_side == position_side
                        and item.type == "LIMIT"
                        and item.time_in_force == "GTC"
                        and item.status not in ("CANCELED", "EXPIRED", "EXPIRED_IN_MATCH")):
                    if ((position_side == "LONG" and item.side == "SELL")
                            or (position_side == "SHORT" and item.side == "BUY")):
                        tp_order = item
                        break

            # 6. Find existing SL order (STOP_MARKET algo, opposite side)
            sl_order = None
            for item in algo_orders:
                if (item.symbol == symbol
                        and item.position_side == position_side
                        and item.order_type == "STOP_MARKET"):
                    sl_order = item
                    break

            # 7. Verify TP: check price and quantity match
            tp_ok = False
            if tp_order:
                tp_percent = Decimal(TP_PERCENT) / Decimal("100")
                if position_side == "LONG":
                    expected_tp_price = entry_price * (Decimal("1") + tp_percent)
                else:
                    expected_tp_price = entry_price * (Decimal("1") - tp_percent)
                expected_tp_price_str = round_price(expected_tp_price, tick_size)
                expected_tp_qty_str = round_quantity(abs(position_amt), step_size)

                # Compare price and quantity as Decimal (not strings!)
                # String comparison fails when exchange returns "982" but
                # round_quantity returns "982.0" — same value, different format.
                actual_price_str = str(tp_order.price) if tp_order.price else ""
                actual_qty_str = str(tp_order.orig_qty) if tp_order.orig_qty else ""
                actual_price_dec = Decimal(actual_price_str) if actual_price_str else Decimal("0")
                actual_qty_dec = Decimal(actual_qty_str) if actual_qty_str else Decimal("0")
                expected_price_dec = Decimal(expected_tp_price_str)
                expected_qty_dec = Decimal(expected_tp_qty_str)

                if actual_price_dec == expected_price_dec and actual_qty_dec == expected_qty_dec:
                    tp_ok = True
                    logging.info(
                        f"[SYNC] {symbol} {position_side}: TP OK "
                        f"orderId={tp_order.order_id} price={actual_price_str} qty={actual_qty_str}"
                    )
                else:
                    logging.warning(
                        f"[SYNC] {symbol} {position_side}: TP MISMATCH — "
                        f"expected price={expected_tp_price_str} qty={expected_tp_qty_str}, "
                        f"actual price={actual_price_str} qty={actual_qty_str} → will replace"
                    )

            # 8. Replace TP if missing or mismatched
            if not tp_ok:
                # Cancel old TP if it exists but is wrong (WS API)
                if tp_order:
                    try:
                        await cancel_tp_order(ws_api_connection, symbol, tp_order.order_id)
                        logging.info(
                            f"[SYNC] {symbol} {position_side}: cancelled old TP "
                            f"orderId={tp_order.order_id}"
                        )
                    except Exception as e:
                        logging.error(
                            f"[SYNC] {symbol} {position_side}: failed to cancel old TP "
                            f"orderId={tp_order.order_id}: {e}"
                        )

                # Place new TP (WS API)
                verify_tp = _make_verify_tp_exists(client, symbol, position_side)
                new_tp_id = await retry_api_call(
                    lambda: place_take_profit(
                        ws_api_connection, symbol, position_side,
                        entry_price, position_amt, tick_size, step_size,
                    ),
                    operation_name=f"sync place TP {symbol} {position_side}",
                    verify_already_placed=verify_tp,
                )
                tp_repaired += 1
                logging.info(
                    f"[SYNC] {symbol} {position_side}: TP placed orderId={new_tp_id}"
                )
            else:
                new_tp_id = tp_order.order_id

            # 9. Check SL
            sl_ok = sl_order is not None
            if sl_ok:
                logging.info(
                    f"[SYNC] {symbol} {position_side}: SL OK "
                    f"algoId={sl_order.algo_id} triggerPrice={sl_order.trigger_price}"
                )

            # 10. Place SL if missing (WS API)
            if not sl_ok:
                # Use current mark price or entry price for SL trigger calculation
                current_mark = entry_price  # fallback
                sl_trigger = compute_sl_trigger(
                    symbol, current_mark, tick_size, position_side,
                )
                verify_sl = _make_verify_sl_exists(client, symbol, position_side)
                new_sl_id = await retry_api_call(
                    lambda: place_stop_loss(
                        ws_api_connection, symbol, position_side, sl_trigger,
                    ),
                    operation_name=f"sync place SL {symbol} {position_side}",
                    verify_already_placed=verify_sl,
                )
                sl_repaired += 1
                logging.info(
                    f"[SYNC] {symbol} {position_side}: SL placed algoId={new_sl_id}"
                )
            else:
                new_sl_id = sl_order.algo_id

            # 11. Update tracking
            # Preserve grid_filled from existing tracking — a grid fill event
            # might have been received by the WS handler during this sync's
            # await calls (e.g., while waiting for position_information_v2).
            # Overwriting True with False would lose the fill signal.
            existing_tracking = tp_sl_tracking.get(key, {})
            tp_sl_tracking[key] = {
                "tp_order_id": new_tp_id if not tp_ok else tp_order.order_id,
                "sl_algo_id": new_sl_id if not sl_ok else sl_order.algo_id,
                "grid_filled": existing_tracking.get("grid_filled", False),
            }

    logging.info(
        f"[SYNC] State sync complete: {positions_checked} positions checked, "
        f"{tp_repaired} TP repaired, {sl_repaired} SL repaired"
    )


# =========================================================================
# Periodic TP/SL Health Check (Feature 3)
# =========================================================================

async def health_check_tp_sl(client, ws_api_connection, tp_sl_tracking: dict, symbol_filters: dict):
    """Periodic health check: verify every open position has TP/SL.

    Same checks as sync_state_with_exchange but ONLY for positions that
    are in tp_sl_tracking. Additionally, check positions NOT in tracking —
    if there's a position but no tracking entry, add tracking + set TP/SL.

    Optimized: one API call per symbol for each of:
    - position_information_v2 (returns ALL positions for symbol — both LONG and SHORT)
    - current_all_open_orders (returns ALL orders for symbol)
    - current_all_algo_open_orders (returns ALL algo orders for symbol)

    Returns a summary: (positions_checked, tp_repaired, sl_repaired)
    """
    tp_repaired = 0
    sl_repaired = 0
    positions_checked = 0

    for symbol in SYMBOLS:
        if symbol not in symbol_filters:
            continue

        tick_size = symbol_filters[symbol]["tick_size"]
        step_size = symbol_filters[symbol]["step_size"]

        # Get ALL positions for this symbol (ONE WS API call for both sides)
        positions = await get_positions_for_symbol(ws_api_connection, symbol)

        # Get ALL open orders for this symbol (ONE REST call)
        open_orders = []
        try:
            response = client.rest_api.current_all_open_orders(
                symbol=symbol, recv_window=5000,
            )
            open_orders = response.data()
        except Exception as e:
            logging.error(f"[HEALTH] current_all_open_orders({symbol}) error: {e}")
            continue

        # Get ALL open algo orders for this symbol (ONE REST call)
        algo_orders = []
        try:
            response = client.rest_api.current_all_algo_open_orders(
                symbol=symbol, recv_window=5000,
            )
            algo_orders = response.data()
        except Exception as e:
            logging.error(f"[HEALTH] current_all_algo_open_orders({symbol}) error: {e}")
            continue

        # Now iterate over position sides in-memory (no more API calls)
        for position_side in ("LONG", "SHORT"):
            key = (symbol, position_side)
            pos = positions.get(position_side)

            if pos is None:
                # No position — clear stale tracking
                if key in tp_sl_tracking:
                    logging.info(
                        f"[HEALTH] {symbol} {position_side}: no position, "
                        f"clearing stale tracking"
                    )
                    del tp_sl_tracking[key]
                continue

            positions_checked += 1
            entry_price = pos["entry_price"]
            position_amt = pos["position_amt"]

            # Find existing TP
            tp_order = None
            for item in open_orders:
                if (item.position_side == position_side
                        and item.type == "LIMIT"
                        and item.time_in_force == "GTC"
                        and item.status not in ("CANCELED", "EXPIRED", "EXPIRED_IN_MATCH")):
                    if ((position_side == "LONG" and item.side == "SELL")
                            or (position_side == "SHORT" and item.side == "BUY")):
                        tp_order = item
                        break

            # Find existing SL
            sl_order = None
            for item in algo_orders:
                if (item.symbol == symbol
                        and item.position_side == position_side
                        and item.order_type == "STOP_MARKET"):
                    sl_order = item
                    break

            # Verify TP price and quantity
            tp_ok = False
            if tp_order:
                tp_percent = Decimal(TP_PERCENT) / Decimal("100")
                if position_side == "LONG":
                    expected_tp_price = entry_price * (Decimal("1") + tp_percent)
                else:
                    expected_tp_price = entry_price * (Decimal("1") - tp_percent)
                expected_tp_price_str = round_price(expected_tp_price, tick_size)
                expected_tp_qty_str = round_quantity(abs(position_amt), step_size)

                # Compare as Decimal (not strings!) — exchange may return
                # "982" while round_quantity returns "982.0"
                actual_price_str = str(tp_order.price) if tp_order.price else ""
                actual_qty_str = str(tp_order.orig_qty) if tp_order.orig_qty else ""
                actual_price_dec = Decimal(actual_price_str) if actual_price_str else Decimal("0")
                actual_qty_dec = Decimal(actual_qty_str) if actual_qty_str else Decimal("0")
                expected_price_dec = Decimal(expected_tp_price_str)
                expected_qty_dec = Decimal(expected_tp_qty_str)

                if actual_price_dec == expected_price_dec and actual_qty_dec == expected_qty_dec:
                    tp_ok = True
                else:
                    logging.warning(
                        f"[HEALTH] {symbol} {position_side}: TP mismatch — "
                        f"expected price={expected_tp_price_str} qty={expected_tp_qty_str}, "
                        f"actual price={actual_price_str} qty={actual_qty_str}"
                    )
            else:
                logging.warning(
                    f"[HEALTH] {symbol} {position_side}: TP MISSING — "
                    f"position exists but no TP order found"
                )

            # Replace TP if needed (WS API)
            if not tp_ok:
                if tp_order:
                    try:
                        await cancel_tp_order(ws_api_connection, symbol, tp_order.order_id)
                        logging.info(
                            f"[HEALTH] {symbol} {position_side}: cancelled wrong TP "
                            f"orderId={tp_order.order_id}"
                        )
                    except Exception as e:
                        logging.error(
                            f"[HEALTH] {symbol} {position_side}: cancel TP failed: {e}"
                        )

                verify_tp = _make_verify_tp_exists(client, symbol, position_side)
                new_tp_id = await retry_api_call(
                    lambda: place_take_profit(
                        ws_api_connection, symbol, position_side,
                        entry_price, position_amt, tick_size, step_size,
                    ),
                    operation_name=f"health place TP {symbol} {position_side}",
                    verify_already_placed=verify_tp,
                )
                tp_repaired += 1

            # Replace SL if needed (WS API)
            if not sl_order:
                logging.warning(
                    f"[HEALTH] {symbol} {position_side}: SL MISSING — "
                    f"position exists but no SL order found"
                )
                current_mark = entry_price  # fallback
                sl_trigger = compute_sl_trigger(
                    symbol, current_mark, tick_size, position_side,
                )
                verify_sl = _make_verify_sl_exists(client, symbol, position_side)
                new_sl_id = await retry_api_call(
                    lambda: place_stop_loss(
                        ws_api_connection, symbol, position_side, sl_trigger,
                    ),
                    operation_name=f"health place SL {symbol} {position_side}",
                    verify_already_placed=verify_sl,
                )
                sl_repaired += 1

            # Update tracking (set or repair)
            tracking = tp_sl_tracking.setdefault(key, {})
            if not tp_ok:
                tracking["tp_order_id"] = new_tp_id
            else:
                tracking["tp_order_id"] = tp_order.order_id
            if not sl_order:
                tracking["sl_algo_id"] = new_sl_id
            else:
                tracking["sl_algo_id"] = sl_order.algo_id
            # Don't overwrite grid_filled=True — a fill event might have been
            # received by the WS handler during this health check's await calls.
            # If grid_filled is already True (from WS handler), preserve it so
            # the main loop will process the fill. Only set False if not True.
            if not tracking.get("grid_filled", False):
                tracking["grid_filled"] = False

    if tp_repaired > 0 or sl_repaired > 0:
        logging.info(
            f"[HEALTH] Health check: {positions_checked} positions checked, "
            f"{tp_repaired} TP repaired, {sl_repaired} SL repaired"
        )
    else:
        logging.info(
            f"[HEALTH] Health check: {positions_checked} positions checked, "
            f"all OK"
        )

    return (positions_checked, tp_repaired, sl_repaired)


# =========================================================================
# Main
# =========================================================================

async def main():
    rest_url, ws_api_url, ws_streams_url = get_urls()
    api_key, api_secret = get_api_keys()

    logging.info(f"Binance USDS-M Futures Bot | environment: {ENVIRONMENT}")
    logging.info(f"REST: {rest_url}")
    logging.info(f"WS API: {ws_api_url}")
    logging.info(f"WS Streams: {ws_streams_url}")
    logging.info(f"Symbols: {SYMBOLS}")
    logging.info(f"Grid step: {GRID_STEP_MODE} base={GRID_BASE_STEP_PERCENT}% | Cancel shift: {GRID_CANCEL_SHIFT_PERCENT}% | Volume mult: {GRID_VOLUME_MULTIPLIER}x")
    logging.info(f"Order sizing: MIN={MIN_ORDER_USD} USD | MAX={MAX_ORDER_USD} USD | "
                 f"Balance usage: {BALANCE_USAGE_PERCENT}%")
    logging.info(f"TP/SL: TP={TP_PERCENT}% from entry | SL={SL_PERCENT}% from last grid level")

    # --- Configuration ---
    # (from examples/rest_api/MarketData/exchange_information.py,
    #  examples/websocket_api/Account/account_information.py,
    #  examples/websocket_streams/mark_price_stream.py)
    configuration_rest_api = ConfigurationRestAPI(
        api_key=api_key,
        api_secret=api_secret,
        base_path=rest_url,
    )
    configuration_ws_api = ConfigurationWebSocketAPI(
        api_key=api_key,
        api_secret=api_secret,
        stream_url=ws_api_url,
    )
    configuration_ws_streams = ConfigurationWebSocketStreams(
        stream_url=ws_streams_url,
    )

    client = DerivativesTradingUsdsFutures(
        config_rest_api=configuration_rest_api,
        config_ws_api=configuration_ws_api,
        config_ws_streams=configuration_ws_streams,
    )

    # =========================================================================
    # STEP 1: Exchange information (REST, no auth required)
    # (from examples/rest_api/MarketData/exchange_information.py)
    #
    # Collect per symbol:
    #   - PRICE_FILTER: tick_size (for price rounding)
    #   - LOT_SIZE: step_size (for quantity rounding), min_qty (minimum quantity)
    #   - MIN_NOTIONAL: notional (minimum order value in USDT)
    # =========================================================================
    symbol_filters = {}

    try:
        response = client.rest_api.exchange_information()

        rate_limits = response.rate_limits
        logging.info(f"exchange_information() rate limits: {rate_limits}")

        data = response.data()
        if data and data.symbols:
            for symbol_info in data.symbols:
                if symbol_info.symbol in SYMBOLS:
                    logging.info(f"--- {symbol_info.symbol} ---")
                    logging.info(f"  pair: {symbol_info.pair}")
                    logging.info(f"  contractType: {symbol_info.contract_type}")
                    logging.info(f"  status: {symbol_info.status}")
                    logging.info(f"  pricePrecision: {symbol_info.price_precision}")
                    logging.info(f"  quantityPrecision: {symbol_info.quantity_precision}")

                    # Collect filters
                    tick_size = None
                    step_size = None
                    min_qty = None
                    min_notional = None

                    if symbol_info.filters:
                        for f in symbol_info.filters:
                            # PRICE_FILTER: tick_size
                            if f.filter_type == "PRICE_FILTER" and f.tick_size is not None:
                                tick_size = Decimal(f.tick_size)
                                logging.info(f"  Filter: PRICE_FILTER tickSize={f.tick_size}")

                            # LOT_SIZE: step_size, min_qty
                            if f.filter_type == "LOT_SIZE":
                                if f.step_size is not None:
                                    step_size = Decimal(f.step_size)
                                    logging.info(f"  Filter: LOT_SIZE stepSize={f.step_size}")
                                if f.min_qty is not None:
                                    min_qty = Decimal(f.min_qty)
                                    logging.info(f"  Filter: LOT_SIZE minQty={f.min_qty}")

                            # MIN_NOTIONAL: notional (minimum order value in USDT)
                            if f.filter_type == "MIN_NOTIONAL" and f.notional is not None:
                                min_notional = Decimal(f.notional)
                                logging.info(f"  Filter: MIN_NOTIONAL notional={f.notional}")

                    # Store all filters for this symbol
                    filters_dict = {}
                    if tick_size is not None:
                        filters_dict["tick_size"] = tick_size
                    if step_size is not None:
                        filters_dict["step_size"] = step_size
                    if min_qty is not None:
                        filters_dict["min_qty"] = min_qty
                    if min_notional is not None:
                        filters_dict["min_notional"] = min_notional

                    if filters_dict:
                        symbol_filters[symbol_info.symbol] = filters_dict
                        logging.info(f"  >>> Filters: {filters_dict}")
                    else:
                        logging.warning(f"  No filters found for {symbol_info.symbol}")

    except Exception as e:
        logging.error(f"exchange_information() error: {e}")

    # Verify we have required filters for all symbols
    for symbol in SYMBOLS:
        if symbol not in symbol_filters:
            logging.error(f"No filters for {symbol} — cannot place grid")
            return
        sf = symbol_filters[symbol]
        if "tick_size" not in sf:
            logging.error(f"No tick_size for {symbol} — cannot round prices")
            return
        if "step_size" not in sf:
            logging.error(f"No step_size for {symbol} — cannot round quantities")
            return
        if "min_qty" not in sf:
            logging.warning(f"No min_qty for {symbol} — will use 0 as minimum quantity")
            sf["min_qty"] = Decimal("0")
        if "min_notional" not in sf:
            logging.warning(f"No min_notional for {symbol} — will use 0 as minimum notional")
            sf["min_notional"] = Decimal("0")

    # =========================================================================
    # STEP 2: Account information via WebSocket API
    # (from examples/websocket_api/Account/account_information.py,
    #  examples/websocket_api/Account/futures_account_balance.py)
    #
    # The WS API connection is kept PERSISTENT for TP/SL/position/balance
    # operations throughout the bot's lifetime (3-channel architecture):
    #   - WS Streams → mark price + user data (handled separately below)
    #   - WS API → TP/SL placement/modification/cancellation + position/balance queries
    #   - REST API → batch grid operations + sync/health check + startup config
    # =========================================================================
    ws_api_connection = None
    try:
        ws_api_connection = await client.websocket_api.create_connection()
        logging.info("WS API connection established for TP/SL operations")

        # --- Balance query removed from startup ---
        # Previously called futures_account_balance_v2() here just for logging,
        # then called get_available_balance() again later for order sizing.
        # This was redundant — the balance will be queried once via
        # get_available_balance() (which also uses futures_account_balance_v2)
        # when calculating order quantities (STEP 6).

    except Exception as e:
        logging.error(f"websocket_api connection error: {e}")

    if ws_api_connection is None:
        logging.error("Failed to create WS API connection — cannot continue. Exiting.")
        return

    # =========================================================================
    # STEP 3: Check and set position mode (REST, USER_DATA)
    # (from examples/rest_api/Account/get_current_position_mode.py
    #  and examples/rest_api/Trade/change_position_mode.py)
    # =========================================================================
    try:
        response = client.rest_api.get_current_position_mode()

        data = response.data()
        current_hedge_mode = data.dual_side_position
        desired_hedge_mode = HEDGE_MODE

        logging.info(f"Position mode: {'Hedge' if current_hedge_mode else 'One-way'} (current) -> "
                     f"{'Hedge' if desired_hedge_mode else 'One-way'} (desired)")

        if current_hedge_mode != desired_hedge_mode:
            response = client.rest_api.change_position_mode(
                dual_side_position="true" if desired_hedge_mode else "false",
            )
            logging.info(f"Position mode changed to {'Hedge' if desired_hedge_mode else 'One-way'}")
        else:
            logging.info(f"Position mode already set: {'Hedge' if current_hedge_mode else 'One-way'}")

    except (ClientError, BadRequestError) as e:
        error_str = str(e)
        if "No need to change" in error_str or "-4046" in error_str:
            logging.info(f"Position mode already set: {'Hedge' if HEDGE_MODE else 'One-way'}")
        else:
            logging.error(f"position_mode error: {e}")
    except Exception as e:
        logging.error(f"position_mode error: {e}")

    # =========================================================================
    # STEP 4: Check and set margin type + leverage per symbol (REST, USER_DATA)
    # (from examples/rest_api/Account/symbol_configuration.py,
    #  examples/rest_api/Trade/change_margin_type.py,
    #  examples/rest_api/Trade/change_initial_leverage.py)
    # =========================================================================
    for symbol in SYMBOLS:
        settings = SYMBOL_SETTINGS.get(symbol)
        if not settings:
            continue

        try:
            response = client.rest_api.symbol_configuration(symbol=symbol)

            data = response.data()
            current_config = None
            for item in data:
                if item.symbol == symbol:
                    current_config = item
                    break

            if current_config:
                logging.info(f"{symbol} config: marginType={current_config.margin_type}, "
                             f"leverage={current_config.leverage}")

                # Check and set margin type
                desired_margin_type = settings["margin_type"]
                if current_config.margin_type != desired_margin_type:
                    try:
                        response = client.rest_api.change_margin_type(
                            symbol=symbol,
                            margin_type=ChangeMarginTypeMarginTypeEnum[desired_margin_type].value,
                        )
                        logging.info(f"{symbol} margin type changed to {desired_margin_type}")
                    except (ClientError, BadRequestError) as e:
                        error_str = str(e)
                        if "No need to change" in error_str or "-4028" in error_str:
                            logging.info(f"{symbol} margin type already set: {desired_margin_type}")
                        else:
                            logging.error(f"{symbol} change_margin_type error: {e}")

                # Check and set leverage
                desired_leverage = settings["leverage"]
                if current_config.leverage != desired_leverage:
                    try:
                        response = client.rest_api.change_initial_leverage(
                            symbol=symbol,
                            leverage=desired_leverage,
                        )
                        logging.info(f"{symbol} leverage changed to {desired_leverage}")
                    except (ClientError, BadRequestError) as e:
                        error_str = str(e)
                        if "No need to change" in error_str or "-4046" in error_str:
                            logging.info(f"{symbol} leverage already set: {desired_leverage}")
                        else:
                            logging.error(f"{symbol} change_initial_leverage error: {e}")

        except Exception as e:
            logging.error(f"symbol_configuration({symbol}) error: {e}")

    # =========================================================================
    # STEP 5: Get initial mark price for each symbol (REST, no auth required)
    # (from examples/rest_api/MarketData/mark_price.py)
    # =========================================================================
    mark_prices = {}
    for symbol in SYMBOLS:
        try:
            response = client.rest_api.mark_price(symbol=symbol)

            data = response.data()
            # MarkPriceResponse uses OpenAPI oneOf pattern — access via .actual_instance
            mark_price_str = data.actual_instance.mark_price
            # mark_price from API is StrictStr — go directly to Decimal, no float
            mark_prices[symbol] = Decimal(mark_price_str)
            logging.info(f"{symbol} mark price: {mark_prices[symbol]}")
        except Exception as e:
            logging.error(f"mark_price({symbol}) error: {e}")

    if not mark_prices:
        logging.error("No mark prices available — cannot place grid. Exiting.")
        return

    # =========================================================================
    # STEP 6: Get available balance and place initial grid orders
    # (from examples/rest_api/Account/futures_account_balance_v3.py,
    #  examples/rest_api/Trade/place_multiple_orders.py)
    # =========================================================================
    base_step = Decimal(GRID_BASE_STEP_PERCENT) / Decimal("100")

    # Center prices — per (symbol, position_side) — the mark price at which
    # the grid was placed/shifted. Per-side because with trend protection,
    # LONG and SHORT grids can be placed at different times and prices.
    center_prices = {}

    # Trend protection: buffers and tracking
    fast_buffers = {}
    slow_buffers = {}
    current_trends = {}
    if TREND_PROTECTION:
        trend_threshold = Decimal(str(TREND_THRESHOLD_PERCENT))
        for symbol in SYMBOLS:
            fast_buffers[symbol] = PriceBuffer(REGRESSION_FAST_WINDOW)
            slow_buffers[symbol] = PriceBuffer(REGRESSION_SLOW_WINDOW)
            current_trends[symbol] = "UNKNOWN"
        logging.info(f"Trend protection: ENABLED | warmup={TREND_WARMUP_MODE} | "
                     f"fast={REGRESSION_FAST_WINDOW}s | slow={REGRESSION_SLOW_WINDOW}s | "
                     f"threshold={TREND_THRESHOLD_PERCENT}%")
    else:
        trend_threshold = Decimal("0")

    # Get available balance
    available_balance = await get_available_balance(ws_api_connection)
    if available_balance <= Decimal("0"):
        logging.error("No available USDT balance — cannot place grid. Exiting.")
        return

    logging.info(f"=== Available balance: {available_balance} USDT ===")

    # =========================================================================
    # STEP 6: Reconcile grid with exchange state + place grid orders
    #
    # Smart startup logic — 3 scenarios per symbol/position_side:
    #
    # A) Grid orders already on exchange from previous session:
    #    → Don't touch them! Restore center_price from existing orders.
    #
    # B) No grid orders, but position exists:
    #    → Calculate how many levels are filled, recover center_price,
    #      and rebuild only the remaining (unfilled) levels.
    #
    # C) No grid orders, no position:
    #    → Place full grid from current mark_price (normal startup).
    # =========================================================================
    for symbol in SYMBOLS:
        settings = SYMBOL_SETTINGS.get(symbol)
        if not settings:
            logging.warning(f"No settings for {symbol}, skipping grid")
            continue

        if symbol not in mark_prices:
            logging.warning(f"No mark price for {symbol}, skipping grid")
            continue

        if symbol not in symbol_filters:
            logging.warning(f"No filters for {symbol}, skipping grid")
            continue

        mark_price = mark_prices[symbol]  # Decimal
        tick_size = symbol_filters[symbol]["tick_size"]  # Decimal
        step_size = symbol_filters[symbol]["step_size"]  # Decimal
        min_qty = symbol_filters[symbol]["min_qty"]  # Decimal
        min_notional = symbol_filters[symbol]["min_notional"]  # Decimal
        leverage = settings["leverage"]  # int

        # Calculate base quantity (level 1) — DCA volume scales per level
        logging.info(f"=== Calculating order size for {symbol} | "
                     f"mark_price={mark_price} | leverage={leverage}x ===")
        quantity_str, notional, skipped = calculate_order_quantity(
            available_balance, symbol, leverage, mark_price,
            step_size, min_qty, min_notional,
        )

        if skipped:
            logging.warning(f"{symbol}: CANNOT place grid — order size below minimum. "
                            f"Need more balance or lower MIN_ORDER_USD.")
            continue

        # Apply trend protection
        if TREND_PROTECTION:
            trend = get_trend(fast_buffers[symbol], slow_buffers[symbol], trend_threshold)
            current_trends[symbol] = trend
            logging.info(f"[TREND] {symbol} initial trend: {trend}")

        # Get ALL open orders for this symbol (ONE REST call for both sides)
        # (from examples/rest_api/Trade/current_all_open_orders.py)
        all_open_orders = []
        try:
            response = client.rest_api.current_all_open_orders(
                symbol=symbol, recv_window=5000,
            )
            all_open_orders = response.data()
        except Exception as e:
            logging.error(f"[STARTUP] current_all_open_orders({symbol}) error: {e}")

        # Get ALL positions for this symbol (ONE WS API call for both sides)
        positions = await get_positions_for_symbol(ws_api_connection, symbol)

        for position_side in ("LONG", "SHORT"):
            key = (symbol, position_side)

            # Trend protection: skip sides that trade against the trend
            if TREND_PROTECTION and not should_trade_side(
                    current_trends.get(symbol, "UNKNOWN"), position_side):
                logging.info(f"[TREND] {symbol}: skipping {position_side} grid — "
                             f"trend is {current_trends.get(symbol, 'UNKNOWN')}")
                continue

            # ─── SCENARIO A: Check for existing grid orders on exchange ───
            grid_side = "BUY" if position_side == "LONG" else "SELL"
            existing_grid = [
                o for o in all_open_orders
                if o.position_side == position_side
                and o.side == grid_side
                and o.type == "LIMIT"
                and o.time_in_force == "GTX"
                and o.status not in ("CANCELED", "EXPIRED", "EXPIRED_IN_MATCH")
            ]

            if existing_grid:
                # Grid orders exist from previous session — don't touch them!
                # Restore center_price from the nearest grid order to mark price.
                # For Fibonacci step: center = nearest_price / (1 ± cum_step_1 * base_step)
                # Level 1 cum_step = 1 for Fibonacci, 1 for geometric
                cum_step_1 = grid_level_cumulative_steps(1)[0]
                if position_side == "LONG":
                    nearest = max(existing_grid, key=lambda o: Decimal(str(o.price)))
                    recovered_center = Decimal(str(nearest.price)) / (Decimal("1") - cum_step_1 * base_step)
                else:
                    nearest = min(existing_grid, key=lambda o: Decimal(str(o.price)))
                    recovered_center = Decimal(str(nearest.price)) / (Decimal("1") + cum_step_1 * base_step)

                center_prices[key] = recovered_center
                logging.info(
                    f"[STARTUP] {symbol} {position_side}: {len(existing_grid)} grid orders "
                    f"already on exchange — restored center_price={recovered_center}"
                )
                continue

            # ─── SCENARIO B: No grid orders, but position exists ───
            pos = positions.get(position_side)

            if pos and pos["position_amt"] != Decimal("0"):
                position_amt = abs(pos["position_amt"])
                entry_price = pos["entry_price"]

                # How many levels are filled?
                # With DCA volume, each level has qty = base_qty * multiplier^(i-1).
                # We determine n_filled by accumulating qty until we reach position_amt.
                base_qty = Decimal(quantity_str)
                vol_mults = grid_level_volume_multipliers(GRID_ORDERS_PER_SIDE)
                accumulated = Decimal("0")
                n_filled = 0
                for i in range(GRID_ORDERS_PER_SIDE):
                    level_qty = base_qty * vol_mults[i]
                    level_qty_rounded = Decimal(round_quantity(level_qty, step_size))
                    accumulated += level_qty_rounded
                    if accumulated <= position_amt + Decimal(round_quantity(base_qty * Decimal("0.1"), step_size)):
                        # Allow small rounding tolerance
                        n_filled = i + 1
                    else:
                        break

                if n_filled >= GRID_ORDERS_PER_SIDE:
                    # All levels filled — no grid needed, TP/SL will close position
                    logging.info(
                        f"[STARTUP] {symbol} {position_side}: all {n_filled} levels filled, "
                        f"no grid needed (TP/SL will close position)"
                    )
                    continue

                # Recover center_price from entry_price and n_filled
                recovered_center = recover_center_price(
                    entry_price, n_filled, position_side
                )

                logging.info(
                    f"[STARTUP] {symbol} {position_side}: {n_filled}/{GRID_ORDERS_PER_SIDE} "
                    f"levels filled → rebuilding levels {n_filled+1}-{GRID_ORDERS_PER_SIDE}, "
                    f"recovered center_price={recovered_center}"
                )

                remaining_orders = build_remaining_grid_orders(
                    symbol, recovered_center, mark_price, tick_size,
                    quantity_str, step_size, position_side, n_filled,
                )
                if remaining_orders:
                    place_orders_batched(client, symbol, remaining_orders)
                center_prices[key] = recovered_center
                continue

            # ─── SCENARIO C: No grid orders, no position → full grid ───
            logging.info(f"=== Building full grid for {symbol} {position_side} | "
                         f"mark_price={mark_price} | base_qty={quantity_str} | "
                         f"step_mode={GRID_STEP_MODE} base_step={GRID_BASE_STEP_PERCENT}% "
                         f"vol_mult={GRID_VOLUME_MULTIPLIER}x ===")

            orders = build_grid_orders(
                symbol, mark_price, tick_size, quantity_str, step_size, position_side
            )
            if orders:
                place_orders_batched(client, symbol, orders)
                center_prices[key] = mark_price
                logging.info(f"{symbol} {position_side} center price set: {mark_price}")

    # =========================================================================
    # STEP 6.5: Initialize shared state + Startup state synchronization
    # Shared state must persist across WS reconnections.
    # Verify all open positions have TP and SL orders.
    # =========================================================================
    latest_prices = {}
    tp_sl_tracking = {}
    grid_replace_needed = set()
    # Cache position amounts from ACCOUNT_UPDATE events.
    # Key: (symbol, position_side), Value: {"entry_price": Decimal, "position_amt": Decimal}
    # Updated from WebSocket events — eliminates REST calls in grid shift checks.
    # When a position closes, the key is removed (amt == 0 means no position).
    position_cache = {}
    # TP modification debounce: avoid rapid-fire modify calls during
    # consecutive partial fills. Key: (symbol, position_side),
    # Value: timestamp of last successful TP modify/place.
    # A new modify is only sent if enough time has passed since the last one.
    last_tp_modify_time = {}
    TP_MODIFY_MIN_INTERVAL = 3  # seconds — minimum between TP modifications
    # Grid shift log throttle: avoid logging "NOT shifting" every second.
    # Key: (symbol, position_side), Value: timestamp of last "NOT shifting" log.
    # Only log once per 30 seconds for the same side.
    last_shift_log_time = {}
    SHIFT_LOG_MIN_INTERVAL = 30  # seconds — minimum between "NOT shifting" logs

    # NOTE: Initial sync moved to AFTER WS streams connection (below, ~line 2540).
    # Running sync before WS is connected causes missed grid fill events —
    # if a fill happens between sync and WS subscription, the event is lost.
    # Sync after WS connection ensures the user data stream is active and
    # all subsequent fill events will be received by the WS handler.

    # =========================================================================
    # STEP 7: Create listen key for User Data Stream
    # (from examples/rest_api/UserDataStreams/start_user_data_stream.py)
    #
    # Listen key is needed to subscribe to account events via WebSocket.
    # Key expires every 60 min — keepalive task extends it every 30 min.
    # =========================================================================
    listen_key = None
    try:
        response = client.rest_api.start_user_data_stream()

        rate_limits = response.rate_limits
        logging.info(f"start_user_data_stream() rate limits: {rate_limits}")

        data = response.data()
        listen_key = data.listen_key
        logging.info(f"Listen key created: {listen_key}")
    except Exception as e:
        logging.error(f"start_user_data_stream() error: {e}")

    if not listen_key:
        logging.error("Cannot create listen key — cannot subscribe to User Data Stream. Exiting.")
        return

    # =========================================================================
    # STEP 8: Mark price stream + User Data Stream + grid management
    # (from examples/websocket_streams/mark_price_stream.py,
    #  examples/rest_api/UserDataStreams/start_user_data_stream.py,
    #  examples/rest_api/UserDataStreams/keepalive_user_data_stream.py,
    #  examples/rest_api/UserDataStreams/close_user_data_stream.py)
    #
    # Streams:
    # - Mark price via WebSocket every ~3 seconds
    # - User Data Stream: ORDER_TRADE_UPDATE, ACCOUNT_UPDATE, listenKeyExpired
    #
    # Grid logic:
    # - If price moved UP by GRID_CANCEL_SHIFT_PERCENT → cancel LONG grid, re-place
    # - If price moved DOWN by GRID_CANCEL_SHIFT_PERCENT → cancel SHORT grid, re-place
    # - Center price updates after each shift
    # - Quantity recalculated from current balance and price on each shift
    # =========================================================================
    cancel_shift = Decimal(GRID_CANCEL_SHIFT_PERCENT) / Decimal("100")  # Decimal shift threshold

    logging.info(f"=== Grid management active | cancel_shift={GRID_CANCEL_SHIFT_PERCENT}% | "
                 f"TP={TP_PERCENT}% | SL={SL_PERCENT}% ===")

    ws_streams_connection = None
    keepalive_task = None
    ws_reconnect_flag = [False]  # [True] when reconnect needed (listenKeyExpired or WS drop)
    reconnect_delay = 5  # seconds, doubles on consecutive failures, max 60
    first_connection = True  # Track whether this is the initial connection
    last_health_check = time.time()

    # =========================================================================
    # Outer reconnection loop — wraps the entire WS lifecycle
    # On WS drop or listenKeyExpired: clean up, create fresh listen key,
    # create new WS connection, resubscribe, sync state.
    # Shared state (latest_prices, tp_sl_tracking, grid_replace_needed,
    # center_prices, fast_buffers, slow_buffers, current_trends) persists
    # across reconnections — only WS connection and listen key are recreated.
    # WS API connection is also recreated on reconnection (3-channel architecture).
    # =========================================================================
    while True:
        try:
            # Recreate WS API connection if it was closed during reconnection
            if ws_api_connection is None:
                try:
                    ws_api_connection = await client.websocket_api.create_connection()
                    logging.info("WS API connection re-established for TP/SL operations")
                except Exception as e:
                    logging.error(f"Failed to re-create WS API connection: {e}")
                    # Wait and retry outer loop
                    reconnect_delay_current = min(reconnect_delay, 60)
                    logging.info(f"Retrying WS API connection in {reconnect_delay_current}s...")
                    await asyncio.sleep(reconnect_delay_current)
                    reconnect_delay = min(reconnect_delay * 2, 60)
                    continue

            ws_streams_connection = await client.websocket_streams.create_connection()

            # -----------------------------------------------------------------
            # Callback for mark price updates
            # (from examples/websocket_streams/mark_price_stream.py)
            # MarkPriceStreamResponse: s=StrictStr (symbol), p=StrictStr (mark price)
            # -----------------------------------------------------------------
            def on_mark_price(data):
                symbol = data.s
                price_str = data.p
                if symbol and price_str:
                    price = Decimal(price_str)
                    latest_prices[symbol] = price
                    # Update trend buffers
                    if TREND_PROTECTION and symbol in fast_buffers:
                        now = time.time()
                        fast_buffers[symbol].add(now, price)
                        slow_buffers[symbol].add(now, price)

            for symbol in SYMBOLS:
                stream = await ws_streams_connection.mark_price_stream(
                    symbol=symbol.lower(),
                )
                stream.on("message", on_mark_price)
            logging.info("Mark price streams subscribed")

            # -----------------------------------------------------------------
            # Subscribe to User Data Stream
            # (from SDK source: websocket_streams/websocket_streams.py — user_data method)
            # Callback receives raw dict (NOT model objects) because:
            #   common/websocket.py line 261-264: oneOf models short-circuit
            #   to raw dict — parsed = payload (no model validation)
            # Event type = data["e"] field (Binance API convention):
            #   "ORDER_TRADE_UPDATE", "ACCOUNT_UPDATE", "listenKeyExpired", etc.
            # Field names in raw dict match the model field names exactly.
            # -----------------------------------------------------------------
            def on_user_data(data):
                """Handle User Data Stream events.

                data is a raw dict (NOT a Pydantic model).
                Event type: data["e"]

                Events we handle:
                - ORDER_TRADE_UPDATE: order fill → set/modify TP and SL
                  data["o"]["s"]=symbol, data["o"]["S"]=side,
                  data["o"]["ps"]=positionSide, data["o"]["X"]=orderStatus
                  data["o"]["m"]=isMaker, data["o"]["p"]=price,
                  data["o"]["q"]=qty, data["o"]["ap"]=avgPrice
                  data["o"]["l"]=lastFilledQty, data["o"]["L"]=lastFilledPrice,
                  data["o"]["n"]=commission, data["o"]["rp"]=realizedPnl
                - ACCOUNT_UPDATE: balance/position change
                  data["a"]["m"]=reason, data["a"]["B"]=balances,
                  data["a"]["P"]=positions
                - listenKeyExpired: need to recreate listen key → trigger reconnect
                """
                if not isinstance(data, dict):
                    logging.warning(f"[USER_DATA] Unexpected data type: {type(data)} — skipping")
                    return

                event_type = data.get("e")

                if event_type == "ORDER_TRADE_UPDATE":
                    o = data.get("o")
                    if o is None:
                        return

                    symbol = o.get("s")
                    side = o.get("S")
                    position_side = o.get("ps")
                    order_status = o.get("X")
                    is_maker = o.get("m")
                    order_price = o.get("p")
                    order_qty = o.get("q")
                    filled_qty = o.get("l")
                    filled_price = o.get("L")
                    commission = o.get("n")
                    commission_asset = o.get("N")
                    realized_pnl = o.get("rp")
                    order_id = o.get("i")
                    order_type = o.get("o")
                    time_in_force = o.get("f")  # GTX=grid, GTC=TP

                    logging.info(
                        f"[ORDER_TRADE_UPDATE] {symbol} {side} {position_side} "
                        f"orderId={order_id} type={order_type} tif={time_in_force} "
                        f"status={order_status} "
                        f"price={order_price} qty={order_qty} "
                        f"filled={filled_qty}@{filled_price} "
                        f"commission={commission}{commission_asset} "
                        f"realizedPnl={realized_pnl} "
                        f"isMaker={is_maker}"
                    )

                    # Verify: our GTX grid orders must ALWAYS be MAKER
                    if (order_status in ("FILLED", "PARTIALLY_FILLED")
                            and order_type == "LIMIT" and is_maker is False
                            and time_in_force == "GTX"
                            and filled_qty != "0"):
                        logging.error(
                            f"!!! NON-MAKER FILL on {symbol} {side} {position_side} "
                            f"orderId={order_id} type={order_type} tif={time_in_force} — "
                            f"taker fill detected! GTX grid orders should never fill as taker."
                        )

                    # === TP/SL LOGIC ===
                    if symbol in SYMBOLS and position_side in ("LONG", "SHORT"):
                        is_grid_fill = (
                            order_status in ("FILLED", "PARTIALLY_FILLED")
                            and time_in_force == "GTX"
                            and order_type == "LIMIT"
                            and filled_qty != "0"
                            and ((position_side == "LONG" and side == "BUY")
                                 or (position_side == "SHORT" and side == "SELL"))
                        )

                        is_tp_fill = (
                            order_type == "LIMIT"
                            and time_in_force == "GTC"
                            and ((position_side == "LONG" and side == "SELL")
                                 or (position_side == "SHORT" and side == "BUY"))
                        )

                        if is_grid_fill:
                            key = (symbol, position_side)
                            tp_sl_tracking.setdefault(key, {})["grid_filled"] = True
                            logging.info(f"[TP/SL] Grid fill detected: {symbol} {position_side} "
                                         f"→ will set/modify TP and SL")

                        elif is_tp_fill and order_status == "EXPIRED_IN_MATCH":
                            key = (symbol, position_side)
                            tp_sl_tracking.setdefault(key, {})["tp_expired"] = True
                            tracking = tp_sl_tracking.get(key)
                            if tracking:
                                tracking["tp_order_id"] = 0
                            logging.warning(
                                f"[TP/SL] TP order EXPIRED_IN_MATCH: {symbol} {position_side} "
                                f"orderId={order_id} price={order_price} → will replace TP"
                            )

                        elif is_tp_fill and order_status in ("FILLED", "PARTIALLY_FILLED"):
                            key = (symbol, position_side)
                            tracking = tp_sl_tracking.get(key)
                            if tracking and order_status == "FILLED":
                                logging.info(f"[TP/SL] TP fully filled: {symbol} {position_side} "
                                             f"orderId={order_id} → position should close")
                                tracking["tp_order_id"] = 0
                            elif tracking and order_status == "PARTIALLY_FILLED":
                                logging.info(f"[TP/SL] TP partially filled: {symbol} {position_side} "
                                             f"orderId={order_id} filled={filled_qty}@{filled_price}")

                elif event_type == "ACCOUNT_UPDATE":
                    a = data.get("a")
                    if a is None:
                        return

                    reason = a.get("m")
                    logging.info(f"[ACCOUNT_UPDATE] reason={reason}")

                    balances = a.get("B")
                    if balances:
                        for b in balances:
                            if b.get("a") == "USDT":
                                logging.info(
                                    f"  Balance: asset={b.get('a')} walletBalance={b.get('wb')} "
                                    f"crossWalletBalance={b.get('cw')} balanceChange={b.get('bc')}"
                                )

                    positions = a.get("P")
                    if positions:
                        for p in positions:
                            if p.get("s") in SYMBOLS:
                                logging.info(
                                    f"  Position: symbol={p.get('s')} positionAmt={p.get('pa')} "
                                    f"entryPrice={p.get('ep')} unrealizedPnl={p.get('up')} "
                                    f"marginType={p.get('mt')} positionSide={p.get('ps')}"
                                )

                    if positions:
                        for p in positions:
                            if p.get("s") in SYMBOLS and p.get("ps") in ("LONG", "SHORT"):
                                pa = p.get("pa")
                                pos_amt = Decimal(pa) if pa else Decimal("0")
                                key = (p.get("s"), p.get("ps"))
                                if pos_amt == Decimal("0"):
                                    # Position closed — remove from cache and tracking
                                    position_cache.pop(key, None)
                                    last_tp_modify_time.pop(key, None)
                                    if key in tp_sl_tracking:
                                        logging.info(f"[TP/SL] Position closed: {p.get('s')} {p.get('ps')} "
                                                     f"→ clearing TP/SL tracking")
                                        del tp_sl_tracking[key]
                                    grid_replace_needed.add(key)
                                    logging.info(f"[GRID] Position closed: {p.get('s')} {p.get('ps')} "
                                                 f"→ will re-place grid")
                                else:
                                    # Update position cache (entry_price, position_amt)
                                    ep = p.get("ep")
                                    entry_p = Decimal(ep) if ep else Decimal("0")
                                    position_cache[key] = {
                                        "entry_price": entry_p,
                                        "position_amt": pos_amt,
                                    }

                elif event_type == "listenKeyExpired":
                    expired_key = data.get("listenKey", "unknown")
                    logging.warning(
                        f"[LISTEN_KEY_EXPIRED] listenKey={expired_key} "
                        f"— setting reconnect flag!"
                    )
                    ws_reconnect_flag[0] = True

                else:
                    logging.info(f"[USER_DATA] {event_type}: {data}")

            user_stream = await ws_streams_connection.user_data(listen_key)
            user_stream.on("message", on_user_data)
            logging.info("User Data Stream subscribed")

            # -----------------------------------------------------------------
            # Keepalive task — extend listen key every 30 minutes
            # (from examples/rest_api/UserDataStreams/keepalive_user_data_stream.py)
            # Listen key expires after 60 min, so we renew at 30 min
            # -----------------------------------------------------------------
            async def keepalive_listen_key():
                """Periodically extend listen key validity."""
                while True:
                    await asyncio.sleep(30 * 60)  # 30 minutes
                    try:
                        response = client.rest_api.keepalive_user_data_stream()

                        rate_limits = response.rate_limits
                        logging.info(f"keepalive_user_data_stream() rate limits: {rate_limits}")

                        logging.info("Listen key keepalive sent")
                    except Exception as e:
                        logging.error(f"keepalive_user_data_stream() error: {e}")

            keepalive_task = asyncio.create_task(keepalive_listen_key())

            # Sync state after WS streams are connected — on BOTH first connection
            # and reconnection. This ensures the user data stream is active and
            # no grid fill events are missed. Previously the initial sync ran
            # BEFORE WS connection, which caused missed fills.
            await sync_state_with_exchange(client, ws_api_connection, tp_sl_tracking, symbol_filters, position_cache)
            first_connection = False

            # Main loop: check prices, manage grid, handle TP/SL
            while True:
                await asyncio.sleep(1)  # Check every second

                # Check if reconnect is needed (listenKeyExpired or external signal)
                if ws_reconnect_flag[0]:
                    logging.warning("[RECONNECT] Reconnect flag set — breaking main loop for reconnection")
                    break

                # === TP/SL: handle signals from on_user_data callback ===
                for symbol in SYMBOLS:
                    for position_side in ("LONG", "SHORT"):
                        key = (symbol, position_side)
                        tracking = tp_sl_tracking.get(key)
                        if not tracking:
                            continue

                        # --- Handle TP expired (EXPIRED_IN_MATCH) ---
                        if tracking.get("tp_expired"):
                            tracking["tp_expired"] = False

                            if symbol not in symbol_filters:
                                continue
                            tick_size = symbol_filters[symbol]["tick_size"]
                            step_size = symbol_filters[symbol]["step_size"]

                            pos_result = await get_positions_for_symbol(ws_api_connection, symbol)
                            pos = pos_result.get(position_side)
                            if pos is None or pos["position_amt"] == Decimal("0"):
                                logging.info(f"[TP/SL] TP expired but no {position_side} position "
                                             f"for {symbol} — skipping replacement")
                                continue

                            entry_price = pos["entry_price"]
                            position_amt = pos["position_amt"]
                            current_mark = latest_prices.get(symbol, entry_price)

                            tp_percent = Decimal(TP_PERCENT) / Decimal("100")
                            if position_side == "LONG":
                                tp_price = entry_price * (Decimal("1") + tp_percent)
                                tp_achievable = current_mark < tp_price
                            else:
                                tp_price = entry_price * (Decimal("1") - tp_percent)
                                tp_achievable = current_mark > tp_price

                            if tp_achievable:
                                logging.info(f"[TP/SL] Replacing expired TP for {symbol} {position_side}: "
                                             f"entry={entry_price} amt={position_amt} "
                                             f"tp_price={round_price(tp_price, tick_size)}")
                                verify_tp = _make_verify_tp_exists(client, symbol, position_side)
                                new_tp_id = await retry_api_call(
                                    lambda: place_take_profit(
                                        ws_api_connection, symbol, position_side,
                                        entry_price, position_amt,
                                        tick_size, step_size,
                                    ),
                                    operation_name=f"replace TP {symbol} {position_side}",
                                    verify_already_placed=verify_tp,
                                )
                                if new_tp_id:
                                    tracking["tp_order_id"] = new_tp_id
                                else:
                                    logging.error(f"[TP/SL] Failed to replace TP for "
                                                  f"{symbol} {position_side}")
                            else:
                                # Price already past TP — close position at market
                                logging.warning(
                                    f"[TP/SL] TP expired AND price past TP level for "
                                    f"{symbol} {position_side} (mark={current_mark}, "
                                    f"tp={round_price(tp_price, tick_size)}) → "
                                    f"market closing position"
                                )
                                try:
                                    if position_side == "LONG":
                                        close_side = NewOrderSideEnum["SELL"].value
                                        close_ps = NewOrderPositionSideEnum["LONG"].value
                                    else:
                                        close_side = NewOrderSideEnum["BUY"].value
                                        close_ps = NewOrderPositionSideEnum["SHORT"].value
                                    close_qty = abs(position_amt)
                                    close_qty_str = round_quantity(close_qty, step_size)

                                    async def _do_market_close(s=symbol, cs=close_side, cp=close_ps, cq=close_qty_str, ws=ws_api_connection):
                                        # Use WS API for market close (TP/SL operations)
                                        response = await ws.new_order(
                                            symbol=s, side=cs, type="MARKET",
                                            position_side=cp, quantity=float(cq),
                                            new_order_resp_type="ACK", recv_window=5000,
                                        )
                                        result = response.data().result
                                        return result.order_id if result and result.order_id else 0

                                    close_result = await retry_api_call(
                                        _do_market_close,
                                        operation_name=f"market close {symbol} {position_side}",
                                    )
                                    if close_result:
                                        logging.info(
                                            f"[TP/SL] Market close: orderId={close_result} "
                                            f"{symbol} {position_side} qty={close_qty_str}"
                                        )
                                    else:
                                        logging.error(
                                            f"[TP/SL] Market close failed for "
                                            f"{symbol} {position_side} after retries"
                                        )
                                except Exception as e:
                                    logging.error(
                                        f"[TP/SL] Market close failed for "
                                        f"{symbol} {position_side}: {e}"
                                    )

                        # --- Handle grid fill → set/modify TP and SL ---
                        if not tracking.get("grid_filled"):
                            continue

                        # Clear the flag immediately to avoid re-processing
                        tracking["grid_filled"] = False

                        if symbol not in symbol_filters:
                            continue

                        tick_size = symbol_filters[symbol]["tick_size"]
                        step_size = symbol_filters[symbol]["step_size"]

                        # Check if this is first fill (no TP/SL yet) or subsequent (modify TP)
                        has_tp = tracking.get("tp_order_id", 0) != 0
                        has_sl = tracking.get("sl_algo_id", 0) != 0

                        # Debounce: for SUBSEQUENT fills (not first), don't modify TP
                        # more often than once per TP_MODIFY_MIN_INTERVAL seconds.
                        # Rapid partial fills (e.g., 6.8, 7.2, 7.5 per second) would
                        # otherwise trigger a modify on each — wasting API calls and
                        # risking -5027 errors. The TP price only changes when entry_price
                        # changes, and qty only when position_amt changes. Batching
                        # these updates is more efficient and safer.
                        if has_tp or has_sl:
                            now = time.time()
                            last_modify = last_tp_modify_time.get(key, 0)
                            if now - last_modify < TP_MODIFY_MIN_INTERVAL:
                                # Too soon — set flag back so we retry next iteration
                                tracking["grid_filled"] = True
                                continue

                        # Get current position from exchange (needed for accurate entry_price)
                        pos_result = await get_positions_for_symbol(ws_api_connection, symbol)
                        pos = pos_result.get(position_side)
                        if pos is None:
                            logging.info(f"[TP/SL] No {position_side} position for {symbol} — skipping")
                            continue

                        entry_price = pos["entry_price"]
                        position_amt = pos["position_amt"]

                        # Update position cache with latest data
                        position_cache[key] = {
                            "entry_price": entry_price,
                            "position_amt": position_amt,
                        }

                        if not has_tp and not has_sl:
                            # === First fill: place BOTH TP and SL ===
                            logging.info(f"[TP/SL] First fill for {symbol} {position_side}: "
                                         f"entry={entry_price} amt={position_amt} "
                                         f"→ placing TP and SL")

                            # Place SL (STOP_MARKET close_position=true)
                            current_mark = latest_prices.get(symbol, entry_price)
                            sl_trigger = compute_sl_trigger(
                                symbol, current_mark, tick_size, position_side
                            )
                            verify_sl = _make_verify_sl_exists(client, symbol, position_side)
                            sl_algo_id = await retry_api_call(
                                lambda: place_stop_loss(
                                    ws_api_connection, symbol, position_side, sl_trigger
                                ),
                                operation_name=f"place SL {symbol} {position_side}",
                                verify_already_placed=verify_sl,
                            )
                            tracking["sl_algo_id"] = sl_algo_id

                            # Place TP (LIMIT GTC for entire position)
                            verify_tp = _make_verify_tp_exists(client, symbol, position_side)
                            tp_order_id = await retry_api_call(
                                lambda: place_take_profit(
                                    ws_api_connection, symbol, position_side,
                                    entry_price, position_amt,
                                    tick_size, step_size,
                                ),
                                operation_name=f"place TP {symbol} {position_side}",
                                verify_already_placed=verify_tp,
                            )
                            tracking["tp_order_id"] = tp_order_id
                            # Record time of TP placement for debouncing
                            last_tp_modify_time[key] = time.time()

                        else:
                            # === Subsequent fill: modify TP (SL stays — closePosition=true) ===
                            logging.info(f"[TP/SL] Subsequent fill for {symbol} {position_side}: "
                                         f"entry={entry_price} amt={position_amt} "
                                         f"→ modifying TP")

                            tp_order_id = tracking.get("tp_order_id", 0)
                            if tp_order_id:
                                success = await modify_take_profit(
                                    ws_api_connection, symbol, position_side,
                                    tp_order_id, entry_price, position_amt,
                                    tick_size, step_size,
                                )
                                if success:
                                    # Record time of TP modification for debouncing
                                    last_tp_modify_time[key] = time.time()
                                else:
                                    logging.warning(f"[TP/SL] Failed to modify TP for {symbol} {position_side} "
                                                    f"→ cancelling old TP and placing new one")
                                    # Cancel old TP before placing new — prevents duplicates!
                                    if tp_order_id:
                                        try:
                                            await cancel_tp_order(ws_api_connection, symbol, tp_order_id)
                                            logging.info(f"[TP/SL] Cancelled old TP orderId={tp_order_id}")
                                        except Exception as cancel_err:
                                            logging.warning(f"[TP/SL] Failed to cancel old TP "
                                                            f"orderId={tp_order_id}: {cancel_err}")
                                    verify_tp = _make_verify_tp_exists(client, symbol, position_side)
                                    new_tp_id = await retry_api_call(
                                        lambda: place_take_profit(
                                            ws_api_connection, symbol, position_side,
                                            entry_price, position_amt,
                                            tick_size, step_size,
                                        ),
                                        operation_name=f"replace TP after modify fail {symbol} {position_side}",
                                        verify_already_placed=verify_tp,
                                    )
                                    if new_tp_id:
                                        tracking["tp_order_id"] = new_tp_id
                                        last_tp_modify_time[key] = time.time()
                                    else:
                                        logging.error(f"[TP/SL] Failed to place new TP for {symbol} {position_side}")
                            else:
                                logging.warning(f"[TP/SL] No TP orderId for {symbol} {position_side} "
                                                f"→ placing new TP")
                                verify_tp = _make_verify_tp_exists(client, symbol, position_side)
                                new_tp_id = await retry_api_call(
                                    lambda: place_take_profit(
                                        ws_api_connection, symbol, position_side,
                                        entry_price, position_amt,
                                        tick_size, step_size,
                                    ),
                                    operation_name=f"place TP no id {symbol} {position_side}",
                                    verify_already_placed=verify_tp,
                                )
                                tracking["tp_order_id"] = new_tp_id
                                if new_tp_id:
                                    last_tp_modify_time[key] = time.time()

                # === Grid re-placement after position close (TP or SL) ===
                if grid_replace_needed:
                    # Take a snapshot and clear — avoid re-processing
                    to_replace = list(grid_replace_needed)
                    grid_replace_needed.clear()

                    for (symbol, position_side) in to_replace:
                        if symbol not in latest_prices:
                            logging.info(f"[GRID] No mark price for {symbol} — skipping re-placement")
                            grid_replace_needed.add((symbol, position_side))  # retry next loop
                            continue
                        if symbol not in symbol_filters:
                            logging.info(f"[GRID] No filters for {symbol} — skipping re-placement")
                            grid_replace_needed.add((symbol, position_side))
                            continue

                        # Trend protection: don't re-place grid against trend
                        if TREND_PROTECTION:
                            trend = get_trend(fast_buffers[symbol], slow_buffers[symbol], trend_threshold)
                            current_trends[symbol] = trend
                            if not should_trade_side(trend, position_side):
                                logging.info(f"[TREND] {symbol} {position_side}: skipping grid re-placement — trend is {trend}")
                                center_prices.pop((symbol, position_side), None)
                                continue

                        current_price = latest_prices[symbol]
                        tick_size = symbol_filters[symbol]["tick_size"]
                        step_size = symbol_filters[symbol]["step_size"]
                        min_qty = symbol_filters[symbol]["min_qty"]
                        min_notional = symbol_filters[symbol]["min_notional"]

                        settings = SYMBOL_SETTINGS.get(symbol)
                        if not settings:
                            continue
                        leverage = settings["leverage"]

                        logging.info(f"[GRID] Re-placing {position_side} grid for {symbol} "
                                     f"from mark_price={current_price}")

                        # 1. Cancel any remaining grid orders for this side
                        await cancel_grid_side(client, symbol, position_side)

                        # 2. Get fresh balance and calculate quantity
                        available_balance = await get_available_balance(ws_api_connection)
                        quantity_str, notional, skipped = calculate_order_quantity(
                            available_balance, symbol, leverage, current_price,
                            step_size, min_qty, min_notional,
                        )

                        if skipped:
                            logging.warning(f"[GRID] Cannot re-place {position_side} grid for {symbol} — "
                                            f"order size below minimum. Grid side left empty!")
                            center_prices.pop((symbol, position_side), None)
                        else:
                            # 3. Build and place new grid for this side
                            grid_orders = build_grid_orders(
                                symbol, current_price, tick_size, quantity_str, step_size, position_side
                            )
                            place_orders_batched(client, symbol, grid_orders)
                            # 4. Update center price per side
                            center_prices[(symbol, position_side)] = current_price
                            logging.info(f"[GRID] {symbol} {position_side} center price updated: {current_price}")

                # === Trend protection: detect changes and place/cancel grids ===
                if TREND_PROTECTION:
                    for symbol in SYMBOLS:
                        if symbol not in symbol_filters:
                            continue
                        if symbol not in latest_prices:
                            continue

                        trend = get_trend(fast_buffers[symbol], slow_buffers[symbol], trend_threshold)
                        prev_trend = current_trends.get(symbol, "UNKNOWN")
                        current_trends[symbol] = trend

                        if trend != prev_trend:
                            logging.info(f"[TREND] {symbol} trend changed: {prev_trend} → {trend}")

                        for position_side in ("LONG", "SHORT"):
                            key = (symbol, position_side)

                            if not should_trade_side(trend, position_side):
                                # Use cached position data instead of REST call
                                cached_pos = position_cache.get(key)
                                if cached_pos is None or cached_pos["position_amt"] == Decimal("0"):
                                    if key in center_prices:
                                        logging.info(f"[TREND] {symbol} {position_side}: "
                                                     f"trend={trend}, no position → cancelling grid")
                                        await cancel_grid_side(client, symbol, position_side)
                                        del center_prices[key]
                                continue

                            if key in center_prices:
                                continue  # Grid already exists

                            # Use cached position data instead of REST call
                            cached_pos = position_cache.get(key)
                            if cached_pos and cached_pos["position_amt"] != Decimal("0"):
                                continue

                            # No position, no grid, trend allows → place grid
                            current_price = latest_prices[symbol]
                            tick_size = symbol_filters[symbol]["tick_size"]
                            step_size = symbol_filters[symbol]["step_size"]
                            min_qty = symbol_filters[symbol]["min_qty"]
                            min_notional = symbol_filters[symbol]["min_notional"]
                            settings = SYMBOL_SETTINGS.get(symbol)
                            if not settings:
                                continue
                            leverage = settings["leverage"]

                            available_balance = await get_available_balance(ws_api_connection)
                            quantity_str, notional_val, skipped = calculate_order_quantity(
                                available_balance, symbol, leverage, current_price,
                                step_size, min_qty, min_notional,
                            )

                            if not skipped:
                                grid_orders = build_grid_orders(
                                    symbol, current_price, tick_size, quantity_str, step_size, position_side
                                )
                                place_orders_batched(client, symbol, grid_orders)
                                center_prices[key] = current_price
                                logging.info(f"[TREND] {symbol} {position_side} grid placed at "
                                             f"{current_price} (trend={trend})")

                # === Grid shift logic (per-side center prices) ===
                for symbol in SYMBOLS:
                    if symbol not in latest_prices:
                        continue
                    if symbol not in symbol_filters:
                        continue

                    settings = SYMBOL_SETTINGS.get(symbol)
                    if not settings:
                        continue

                    current_price = latest_prices[symbol]
                    tick_size = symbol_filters[symbol]["tick_size"]
                    step_size = symbol_filters[symbol]["step_size"]
                    min_qty = symbol_filters[symbol]["min_qty"]
                    min_notional = symbol_filters[symbol]["min_notional"]
                    leverage = settings["leverage"]
                    trend = current_trends.get(symbol, "NEUTRAL") if TREND_PROTECTION else "NEUTRAL"

                    # --- Check LONG grid shift ---
                    long_key = (symbol, "LONG")
                    if long_key in center_prices:
                        center_long = center_prices[long_key]
                        shift_pct_long = (current_price - center_long) / center_long

                        if shift_pct_long >= cancel_shift:
                            # Use cached position data from ACCOUNT_UPDATE events
                            # instead of REST API call (eliminates ~1 REST call/second)
                            cached_long = position_cache.get(long_key)
                            if cached_long and cached_long["position_amt"] != Decimal("0"):
                                # Throttle: only log "NOT shifting" once per 30 seconds
                                now = time.time()
                                last_log = last_shift_log_time.get(long_key, 0)
                                if now - last_log >= SHIFT_LOG_MIN_INTERVAL:
                                    logging.info(f">>> {symbol} LONG: price UP {shift_pct_long * Decimal('100'):.4f}% "
                                                 f"(threshold {GRID_CANCEL_SHIFT_PERCENT}%) "
                                                 f"→ NOT shifting — open LONG position "
                                                 f"(amt={cached_long['position_amt']})")
                                    last_shift_log_time[long_key] = now
                            elif not should_trade_side(trend, "LONG"):
                                logging.info(f"[TREND] {symbol} LONG: price UP {shift_pct_long * Decimal('100'):.4f}% "
                                             f"→ NOT shifting — trend={trend} blocks LONG")
                            else:
                                logging.info(f">>> {symbol} LONG: price UP {shift_pct_long * Decimal('100'):.4f}% "
                                             f"(threshold {GRID_CANCEL_SHIFT_PERCENT}%) "
                                             f"→ cancelling LONG grid, re-placing from {current_price}")

                                cancelled = await cancel_grid_side(client, symbol, "LONG")
                                if cancelled > 0:
                                    available_balance = await get_available_balance(ws_api_connection)
                                    quantity_str, notional, skipped = calculate_order_quantity(
                                        available_balance, symbol, leverage, current_price,
                                        step_size, min_qty, min_notional,
                                    )

                                    if skipped:
                                        logging.warning(f"{symbol}: cannot re-place LONG grid — "
                                                        f"order size below minimum. Grid side left empty!")
                                        del center_prices[long_key]
                                    else:
                                        long_orders = build_grid_orders(
                                            symbol, current_price, tick_size, quantity_str, step_size, "LONG"
                                        )
                                        place_orders_batched(client, symbol, long_orders)
                                        center_prices[long_key] = current_price
                                        logging.info(f"{symbol} LONG center price updated: {current_price}")
                                else:
                                    logging.info(f"{symbol}: no LONG orders cancelled, center unchanged")

                    # --- Check SHORT grid shift ---
                    short_key = (symbol, "SHORT")
                    if short_key in center_prices:
                        center_short = center_prices[short_key]
                        shift_pct_short = (current_price - center_short) / center_short

                        if shift_pct_short <= -cancel_shift:
                            # Use cached position data from ACCOUNT_UPDATE events
                            # instead of REST API call (eliminates ~1 REST call/second)
                            cached_short = position_cache.get(short_key)
                            if cached_short and cached_short["position_amt"] != Decimal("0"):
                                # Throttle: only log "NOT shifting" once per 30 seconds
                                now = time.time()
                                last_log = last_shift_log_time.get(short_key, 0)
                                if now - last_log >= SHIFT_LOG_MIN_INTERVAL:
                                    logging.info(f">>> {symbol} SHORT: price DOWN {shift_pct_short * Decimal('100'):.4f}% "
                                                 f"(threshold -{GRID_CANCEL_SHIFT_PERCENT}%) "
                                                 f"→ NOT shifting — open SHORT position "
                                                 f"(amt={cached_short['position_amt']})")
                                    last_shift_log_time[short_key] = now
                            elif not should_trade_side(trend, "SHORT"):
                                logging.info(f"[TREND] {symbol} SHORT: price DOWN {shift_pct_short * Decimal('100'):.4f}% "
                                             f"→ NOT shifting — trend={trend} blocks SHORT")
                            else:
                                logging.info(f">>> {symbol} SHORT: price DOWN {shift_pct_short * Decimal('100'):.4f}% "
                                             f"(threshold -{GRID_CANCEL_SHIFT_PERCENT}%) "
                                             f"→ cancelling SHORT grid, re-placing from {current_price}")

                                cancelled = await cancel_grid_side(client, symbol, "SHORT")
                                if cancelled > 0:
                                    available_balance = await get_available_balance(ws_api_connection)
                                    quantity_str, notional, skipped = calculate_order_quantity(
                                        available_balance, symbol, leverage, current_price,
                                        step_size, min_qty, min_notional,
                                    )

                                    if skipped:
                                        logging.warning(f"{symbol}: cannot re-place SHORT grid — "
                                                        f"order size below minimum. Grid side left empty!")
                                        del center_prices[short_key]
                                    else:
                                        short_orders = build_grid_orders(
                                            symbol, current_price, tick_size, quantity_str, step_size, "SHORT"
                                        )
                                        place_orders_batched(client, symbol, short_orders)
                                        center_prices[short_key] = current_price
                                        logging.info(f"{symbol} SHORT center price updated: {current_price}")
                                else:
                                    logging.info(f"{symbol}: no SHORT orders cancelled, center unchanged")

                # === Periodic TP/SL health check (every 60 seconds) ===
                now = time.time()
                if now - last_health_check >= 60:
                    await health_check_tp_sl(client, ws_api_connection, tp_sl_tracking, symbol_filters)
                    last_health_check = now

            # === Reconnection handling (broke out of main loop due to reconnect flag) ===
            logging.info("[RECONNECT] Starting reconnection sequence...")

            # 1. Cancel keepalive task
            if keepalive_task and not keepalive_task.done():
                keepalive_task.cancel()
                try:
                    await keepalive_task
                except asyncio.CancelledError:
                    pass

            # 2. Close old WS Streams connection
            if ws_streams_connection:
                try:
                    await ws_streams_connection.close_connection(close_session=True)
                except Exception:
                    pass
                ws_streams_connection = None

            # 2.5 Close old WS API connection (3-channel architecture)
            if ws_api_connection:
                try:
                    await ws_api_connection.close_connection(close_session=True)
                    logging.info("[RECONNECT] Old WS API connection closed")
                except Exception:
                    pass
                ws_api_connection = None

            # 3. Close old listen key
            try:
                client.rest_api.close_user_data_stream()
                logging.info("[RECONNECT] Old listen key closed")
            except Exception as e:
                logging.warning(f"[RECONNECT] close_user_data_stream() error: {e}")

            # 4. Create fresh listen key
            try:
                response = client.rest_api.start_user_data_stream()
                listen_key = response.data().listen_key
                logging.info(f"[RECONNECT] Fresh listen key created: {listen_key}")
            except Exception as e:
                logging.error(f"[RECONNECT] start_user_data_stream() error: {e}")
                # Can't continue without listen key — wait and retry outer loop
                reconnect_delay_current = min(reconnect_delay, 60)
                logging.info(f"[RECONNECT] Retrying in {reconnect_delay_current}s...")
                await asyncio.sleep(reconnect_delay_current)
                reconnect_delay = min(reconnect_delay * 2, 60)
                continue

            # 5. Reset reconnect flag and delay
            ws_reconnect_flag[0] = False
            reconnect_delay = 5  # Reset on successful reconnection steps

            # 6. Sync state after reconnection (will be called at start of next iteration)
            # Continue outer loop → will create new WS connection
            logging.info("[RECONNECT] Reconnection setup complete, creating new WS connection...")
            continue

        except Exception as e:
            logging.error(f"[RECONNECT] WebSocket error: {e}")

            # Cancel keepalive task
            if keepalive_task and not keepalive_task.done():
                keepalive_task.cancel()
                try:
                    await keepalive_task
                except asyncio.CancelledError:
                    pass
                keepalive_task = None

            # Close old WS Streams connection
            if ws_streams_connection:
                try:
                    await ws_streams_connection.close_connection(close_session=True)
                except Exception:
                    pass
                ws_streams_connection = None

            # Close old WS API connection (3-channel architecture)
            if ws_api_connection:
                try:
                    await ws_api_connection.close_connection(close_session=True)
                    logging.info("[RECONNECT] Old WS API connection closed after error")
                except Exception:
                    pass
                ws_api_connection = None

            # Try to close old listen key and create new one
            try:
                client.rest_api.close_user_data_stream()
            except Exception:
                pass

            try:
                response = client.rest_api.start_user_data_stream()
                listen_key = response.data().listen_key
                logging.info(f"[RECONNECT] Fresh listen key created after error: {listen_key}")
            except Exception as e2:
                logging.error(f"[RECONNECT] start_user_data_stream() error after WS failure: {e2}")

            # Reset reconnect flag
            ws_reconnect_flag[0] = False

            # Wait before reconnecting with exponential backoff
            reconnect_delay_current = min(reconnect_delay, 60)
            logging.info(f"[RECONNECT] Reconnecting in {reconnect_delay_current}s...")
            await asyncio.sleep(reconnect_delay_current)
            reconnect_delay = min(reconnect_delay * 2, 60)

            # Continue outer loop
            continue


if __name__ == "__main__":
    asyncio.run(main())
