# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Chinese A-share stock signal analysis system. Scrapes market data via baostock/EastMoney APIs, computes 13 technical indicators (KDJ, MACD, RSI, Bollinger, MA, etc.), detects 18 buy/sell signal types across 8 indicator families, filters by liquidity/valuation/quality gates, and presents results via a Flask web dashboard.

**Language:** Python 3 | **Frameworks:** Scrapy, baostock, pandas, ta, Flask | **DB:** SQLite

## Repository Layout

The git root is `/Users/dingli/Documents/UGit/Spiders/` (one level up from `Spiders/`).

```
Spiders/                          # Scrapy project package
  run.py                          # MAIN ENTRY POINT - orchestrates full pipeline
  spiders/
    stock_kline.py                # Core spider (2157 lines) - data fetching + analysis
    baostock_helper.py            # baostock API layer: login, K-line fetch, stock list
    signal_compute_worker.py      # Multi-process signal detection (pickle-safe, module-level functions)
    technical_indicators.py       # TechnicalIndicators class - all 13 indicator calculations
    stock_config.py               # ALL configuration: APIs, indicator params, signal filters, workers
    get_stock_list.py             # Stock list spider (Juhe API fallback)
    stock_detail.py               # EastMoney real-time stock detail
  web/                            # Flask dashboard (port 5001)
    app.py                        # Flask app with REST APIs
    templates/calendar.html       # Timeline/calendar view
    templates/index.html          # List view
scripts/cloud/                    # CloudBase upload scripts
requirements.txt                  # Python dependencies
stock_signals.db                  # SQLite database
stock_list.txt                    # Cached stock codes (7-day TTL)
```

## Commands

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run the main pipeline
```bash
cd Spiders
python run.py                    # Today's data
python run.py --date 20260501    # Specific date
python run.py --yesterday        # Yesterday's data
```

### Run web dashboard
```bash
cd Spiders/web
python app.py                    # Foreground
./start.sh                       # Background daemon (PID in web.pid)
./stop.sh                        # Stop daemon
```

### Run individual Scrapy spiders
```bash
cd /Users/dingli/Documents/UGit/Spiders
scrapy crawl stock_kline -a use_file=true -a stock_codes=sh603288 -a calc_indicators=true
scrapy crawl stock_list
scrapy crawl stock_detail
```

### Testing
```bash
python scripts/test/test_baostock.py
```
No formal test framework is configured.

## Architecture

### Data Pipeline (run.py orchestrates)
1. **Stock list** - `baostock_helper.py` (primary) or `get_stock_list.py` (Juhe fallback). Cached to `stock_list.txt` (7-day TTL).
2. **K-line fetch** - `baostock_helper.py` via `ProcessPoolExecutor` (default 3 workers). Periodic re-login every 40 requests to avoid connection drops. Jitter/sleep between requests to reduce BrokenPipeErrors.
3. **Indicator + signal computation** - `BAOSTOCK_PIPELINE_FETCH_AND_COMPUTE=True` means each worker fetches then computes in one step. `signal_compute_worker.py` is designed pickle-safe (all module-level functions, no class instances).
4. **Output** - `kdj_signals_YYYYMMDD.txt` (human-readable), `stock_signals.db` (SQLite), Tencent CloudBase (cloud upload).

### Key Design Decisions
- **Worker isolation**: `signal_compute_worker.py` functions must remain module-level for `ProcessPoolExecutor` pickling. Do not add class instances or closures.
- **Cache-first**: Stock lists cached 7 days, valuation data 1 day. `should_refresh_stock_detail_cache()` triggers after 15:30.
- **Signal quality gates**: ST exclusion, trade status check, liquidity filters (min avg amount 1e8, turnover rate 1%, volume ratio 0.7), valuation filters (PE 0-85, PB 0-9), historical success rate thresholds (60%+).
- **Volume heat scoring** (0-100): volume trend (40pts) + recent surge (25pts) + trading amount (35pts), with liquidity discount.

### Configuration
All business logic config lives in `spiders/stock_config.py`: API endpoints, field mappings, indicator parameters, `SIGNAL_FILTERS` dict (liquidity/valuation/quality thresholds), worker pool sizes. This is the single source of truth for tuning signal detection behavior.

### Web Dashboard
Flask app at `web/app.py`. Key APIs: `/api/signals`, `/api/stats`, `/api/calendar/events`, `/api/stock-codes`, `/api/filter-options`, `/api/signal-daily-prices`. Database path resolved relative to repo root: `../../stock_signals.db`.

## Conventions

- Commit messages are in Chinese.
- baostock process pool: keep workers at 3-8 (8 is tested stable, >8 causes BrokenPipeErrors).
- `BAOSTOCK_RELOGIN_EVERY_N_REQUESTS`: 40-150 range recommended. Too low causes server disconnection.
- When modifying signal logic, changes go through `stock_config.py` (thresholds) or `signal_compute_worker.py` (detection logic). The `stock_kline.py` file handles orchestration and I/O.
