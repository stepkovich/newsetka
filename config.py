# ========================================================================
# Bot configuration
# =========================================================================
# All parameters the bot imports from this file.
# Adjust values to your needs before running the bot.
# =========================================================================

# --- Environment ---
# "demo"     — REST = DEMO URL, WS = TESTNET URLs
# "testnet"  — all TESTNET URLs
# "mainnet"  — all PROD URLs
ENVIRONMENT = "demo"

# --- Symbols ---
# List of trading pairs the bot will manage.
SYMBOLS = ["SUIUSDT", "DOGEUSDT"]

# Desired settings per symbol
SYMBOL_SETTINGS = {
    "SUIUSDT": {
        "leverage": 1,
        "margin_type": "CROSSED",  # ISOLATED or CROSSED (must match ChangeMarginTypeMarginTypeEnum)
    },
    "DOGEUSDT": {
        "leverage": 1,
        "margin_type": "CROSSED",
    },
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
GRID_CANCEL_SHIFT_PERCENT = 0.08

# --- Order sizing ---
# Minimum and maximum notional value for the BASE (first) grid order (in USDT).
# With DCA volume, later orders will be larger: level i notional = base * multiplier^(i-1).
# The bot calculates base_qty so that total notional across all levels fits
# the allocated budget (balance * usage% / num_symbols / 2 sides).
MIN_ORDER_USD = 5
MAX_ORDER_USD = 5000

# Percentage of available USDT balance to use for trading.
BALANCE_USAGE_PERCENT = 80

# --- Take-profit and Stop-loss ---
# TP is placed as a percentage from the average entry price.
TP_PERCENT = 0.3

# SL is placed as a percentage below the last grid level
# (the furthest order from mark price).
SL_PERCENT = 1.0

# =========================================================================
# Trend protection (linear regression on mark price)
# =========================================================================

# Enable or disable trend protection.
# When enabled, the bot will not place grid orders against the detected
# trend if there is no existing position on that side.
# True  — trend protection active
# False — trade both sides regardless of trend
TREND_PROTECTION = True

# Warm-up mode controls what happens at bot start when the regression
# buffers are still empty (no data yet).
# "full"     — wait for regression buffers to fill completely before
#              placing any grid. Safer: no trades until trend is known.
# "immediate" — start trading both sides immediately, and let trend
#              protection kick in once enough data is collected.
TREND_WARMUP_MODE = "immediate"

# Fast regression window in seconds.
# Approx ~5 minutes = 300 seconds.
# The fast regression reacts quickly to recent price changes.
REGRESSION_FAST_WINDOW = 300

# Slow regression window in seconds.
# Approx ~20 minutes = 1200 seconds.
# The slow regression determines the primary trend direction.
REGRESSION_SLOW_WINDOW = 1200

# Trend threshold as a percentage.
# If the normalized regression slope exceeds this value, the market is
# considered to be in a trend. Below this value = sideways / NEUTRAL.
# Example: 0.05 means the slow regression must show >= 0.05% change
# over its window to count as a trend.
TREND_THRESHOLD_PERCENT = 0.3
