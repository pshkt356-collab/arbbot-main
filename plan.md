# Plan: MEXC Flip Trading Mode Implementation

## Overview
Add a new trading mode "MEXC Flip Trading" that:
1. Monitors Binance price movements (leading exchange) via WebSocket in real-time
2. Opens LONG positions on MEXC futures when Binance price goes UP
3. Closes positions immediately when Binance price starts going DOWN (minimal movement)
4. Uses high leverage (200-300x) on MEXC zero-fee futures
5. Many quick trades (200-300) accumulate significant profit
6. User can select which pairs to trade

## Architecture Changes

### Stage 1: Core Engine (services/mexc_flip_trader.py)
- New file implementing the MEXC Flip Trading engine
- Binance WebSocket price stream monitoring (tick-level)
- Price direction detection algorithm (using recent price history)
- MEXC API integration for futures trading (open/close LONG positions)
- MEXC WebSocket for order tracking
- Leverage setting (200-300x)
- Position management (rapid open/close cycles)
- PnL tracking per flip session
- Configurable pairs selection
- Risk management (max daily loss, position size limits)

### Stage 2: Database Layer (database/models.py)
- New table `flip_settings` - per-user flip trading settings
  - user_id, enabled, selected_symbols, leverage, position_size_usd
  - max_daily_flips, max_daily_loss_usd, min_price_movement_pct
  - close_on_reverse, test_mode
- New table `flip_trades` - flip trade records
  - id, user_id, symbol, entry_price, exit_price, pnl_usd, pnl_percent
  - leverage, position_size_usd, status, opened_at, closed_at, duration_ms
- Migration methods for new tables
- New dataclass: FlipSettings, FlipTrade

### Stage 3: Configuration (config.py)
- MEXC-specific settings
- Flip trading defaults
- WebSocket endpoints

### Stage 4: Telegram UI (handlers/callbacks.py, handlers/states.py)
- New menu: "🔥 MEXC Flip" in main menu
- Settings submenu:
  - Select pairs to trade (multi-select)
  - Set leverage (200-300x slider)
  - Position size
  - Enable/disable mode
  - Stats display
- New FSM states for flip configuration

### Stage 5: Integration (main.py)
- Initialize flip trader service on startup
- Add to health checks
- Graceful shutdown

### Stage 6: Testing & Validation
- Syntax check all modified files
- Verify no broken existing functionality
- Test integration points

## Files to Modify:
1. **database/models.py** - Add FlipSettings, FlipTrade dataclasses and DB tables
2. **config.py** - Add flip trading configuration
3. **services/mexc_flip_trader.py** - NEW: Core flip trading engine
4. **handlers/callbacks.py** - Add flip trading menu and callbacks
5. **handlers/states.py** - Add FSM states for flip configuration
6. **handlers/commands.py** - Add /flip command
7. **main.py** - Integrate flip trader into startup/shutdown
8. **services/__init__.py** - Export flip trader

## Key Design Decisions:
- Use existing ccxt.async_support for MEXC API calls
- Reuse Binance WebSocket connection from spread_scanner when possible
- Independent WebSocket for MEXC futures price/ordee tracking
- Price direction detection: compare last N ticks, detect trend reversal
- Ultra-fast execution: no order confirmations, fire-and-forget with async tracking
- Separate position table to not interfere with existing arbitrage trades
