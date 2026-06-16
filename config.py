# =========================================================================
# Bot configuration
# =========================================================================
# All parameters the bot imports from this file.
# Adjust values to your needs before running the bot.
# =========================================================================

# --- Environment ---
# "demo"     — REST = DEMO URL, WS = TESTNET URLs
# "testnet"  — all TESTNET URLs
# "mainnet"  — all PROD URLs
ENVIRONMENT = "mainnet"

# --- Symbols ---
# List of trading pairs the bot will manage.
SYMBOLS = ["XRPUSDC"]

# Desired settings per symbol
SYMBOL_SETTINGS = {
    "XRPUSDC": {
        "leverage": 75,
        "margin_type": "CROSSED",  # ISOLATED or CROSSED (must match ChangeMarginTypeMarginTypeEnum)
    },
    # "DOGEUSDT": {
    #     "leverage": 2,
    #     "margin_type": "CROSSED",
    # },
}

# --- Position mode ---
# True = Hedge Mode (LONG and SHORT positions can be open simultaneously)
# False = One-way Mode (only one direction at a time)
HEDGE_MODE = True

# --- Grid settings ---
# Number of limit orders per side (LONG / SHORT)
GRID_ORDERS_PER_SIDE = 12

# Base distance for the first grid level as percentage.
# With Fibonacci step mode, level i gap = fib(i) * GRID_BASE_STEP_PERCENT.
# Level 1 gap = 1 * 0.03% = 0.03%
# Level 5 gap = 5 * 0.03% = 0.15%
# Level 12 gap = 144 * 0.03% = 4.32%
# Total coverage with 12 Fib levels and base_step=0.03% ≈ 11.28%
GRID_BASE_STEP_PERCENT = 0.03

# Grid step mode:
# "fibonacci" — gaps follow Fibonacci sequence: 1, 1, 2, 3, 5, 8, 13, 21, ...
#               Level i gap = fib(i) * GRID_BASE_STEP_PERCENT
# "geometric" — gaps are constant: each level = GRID_BASE_STEP_PERCENT
#               (old behavior, equivalent to GRID_STEP_PERCENT)
GRID_STEP_MODE = "fibonacci"

# Volume multiplier for geometric volume scaling per level.
# qty at level i = base_qty * GRID_VOLUME_MULTIPLIER^(i-1)
# 1.0 = equal volume on all levels (old behavior)
# 1.5 = conservative DCA (level 12 = 86.5x base_qty)
# 2.0 = standard DCA (level 12 = 2048x base_qty)
GRID_VOLUME_MULTIPLIER = 1.5

# How far the price must move from the grid center before the grid is
# cancelled and re-placed at the new price (percentage).
GRID_CANCEL_SHIFT_PERCENT = 0.07

# --- Order sizing ---
# Minimum and maximum notional value for the BASE (first) grid order (in USDT).
# With DCA volume, later orders will be larger: level i notional = base * multiplier^(i-1).
# The bot calculates base_qty so that total notional across all levels fits
# the allocated budget (balance * usage% / num_symbols / 2 sides).
MIN_ORDER_USD = 5.2
MAX_ORDER_USD = 10

# Percentage of available USDT balance to use for trading.
BALANCE_USAGE_PERCENT = 50

# --- Take-profit and Stop-loss ---
# TP is placed as a percentage from the average entry price.
TP_PERCENT = 0.03

# SL is placed as a percentage below the last grid level
# (the furthest order from mark price).
SL_PERCENT = 0.8

# =========================================================================
# Trend protection (Kalman filter on mark price)
# =========================================================================

# Enable or disable trend protection.
# When enabled, the bot will:
#   - Cancel grid orders against the detected trend
#   - Rebuild remaining levels when trend ends (NEUTRAL)
# True  — trend protection active
# False — trade both sides regardless of trend
TREND_PROTECTION = True

# Warm-up mode controls what happens at bot start when the Kalman
# filter hasn't received enough data yet.
# "full"     — wait for KALMAN_MIN_TICKS data points before placing
#              any grid. Safer: no trades until trend is known.
# "immediate" — start trading both sides immediately, and let trend
#              protection kick in once enough data is collected.
TREND_WARMUP_MODE = "full"

# Kalman filter parameters — replace regression windows.
# Only 2 parameters instead of fast/slow windows + threshold.

# Process noise (Q) — how much the price can change on its own per tick.
# Higher = filter reacts faster, but more noise in the trend signal.
# Lower = filter smoother, but slower to detect trend changes.
# Typical range: 0.0001 – 0.01
KALMAN_PROCESS_NOISE = 0.002

# Measurement noise (R) — how noisy the price data is.
# Higher = filter trusts its prediction more, reacts slower (smoother).
# Lower = filter trusts the price data more, reacts faster (noisier).
# With R=0.01, Kalman gain ≈ Q/(Q+R) ≈ 0.17 → only 17% of each tick
# affects the state — much smoother than R=0.001 (50% per tick).
# Typical range: 0.001 – 0.1
KALMAN_MEASUREMENT_NOISE = 0.01

# Minimum number of price ticks before the Kalman filter reports
# a trend. Prevents false signals from just a few data points.
# At ~3 sec/tick, 30 ticks = ~90 seconds of warm-up.
KALMAN_MIN_TICKS = 100

# Trend confirmation: raw UP/DOWN must persist for this many
# consecutive ticks before the filter reports it as confirmed.
# Prevents rapid UP↔DOWN whipsaw on every 3-second tick.
# At ~3 sec/tick, 5 ticks = ~15 seconds of sustained move needed.
# NEUTRAL is reported immediately (no confirmation) — we want to
# rebuild grids as soon as the trend fades.
KALMAN_CONFIRM_TICKS = 5

# Trend threshold as a percentage of price change per tick.
# If the Kalman velocity exceeds this, the market is considered
# to be in a trend. Below this = sideways / NEUTRAL.
# With R=0.01, typical velocity noise is ~0.001-0.005%.
# 0.005% threshold filters out noise while catching real moves.
TREND_THRESHOLD_PERCENT = 0.005

# =========================================================================
# Watchdog
# =========================================================================

# Maximum number of seconds without receiving ANY WebSocket data
# (mark price or user data events) before triggering a reconnect.
# Mark price updates arrive every ~1-3 seconds normally.
# Set this high enough to avoid false triggers during short Binance pauses,
# but low enough to catch a truly dead connection.
# 30 seconds = generous, allows for brief Binance hiccups.
WATCHDOG_TIMEOUT = 30
