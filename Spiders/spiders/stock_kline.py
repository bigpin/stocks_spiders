import scrapy
import json
import csv
import os
import pandas as pd
from datetime import datetime, timedelta
from items import EastMoneyItem
from .stock_config import (
    KLINE_API,
    KLINE_FIELD_MAPPING,
    STOCK_PREFIX_MAP,
    HEADERS,
    INDICATORS_CONFIG,
    SIGNAL_FILTERS,
    DATA_SOURCE,
    BAOSTOCK_FETCH_WORKERS,
)
from .baostock_helper import (
    fetch_kline_data_baostock_simple,
    get_stock_name_baostock,
    login_baostock,
    fetch_one_baostock_worker,
)
from concurrent.futures import ProcessPoolExecutor, as_completed
from .technical_indicators import TechnicalIndicators
import sqlite3
import bisect

class StockKlineSpider(scrapy.Spider):
    name = "stock_kline"
    allowed_domains = ["eastmoney.com", "push2his.eastmoney.com"]
    # custom_settings = {
    #         'FEEDS': {
    #             'kline_data.csv': {
    #                 'format': 'csv',
    #                 'encoding': 'utf-8-sig',
    #                 'store_empty': False,
    #                 'overwrite': True,
    #                 'fields': [
    #                     'stock_code', 'date', 'open', 'high', 'low', 'close', 
    #                     'volume', 'amount', 'amplitude', 'change_rate', 'change_amount', 
    #                     'turnover', 'KST_9_3', 'DST_9_3', 'JST_9_3', 'MACD_12_26_9', 
    #                     'MACDh_12_26_9', 'MACDs_12_26_9', 'RSI_6', 'RSI_12', 'RSI_24', 
    #                     'BBL_20_2.0', 'BBM_20_2.0', 'BBU_20_2.0', 'BBB_20_2.0', 'BBP_20_2.0'
    #                 ],
    #                 'headers': {
    #                     'stock_code': '股票代码',
    #                     'date': '日期',
    #                     'open': '开盘价',
    #                     'high': '最高价',
    #                     'low': '最低价',
    #                     'close': '收盘价',
    #                     'volume': '成交量',
    #                     'amount': '成交额',
    #                     'amplitude': '振幅',
    #                     'change_rate': '涨跌幅',
    #                     'change_amount': '涨跌额',
    #                     'turnover': '换手率',
    #                     'KST_9_3': 'K值',
    #                     'DST_9_3': 'D值',
    #                     'JST_9_3': 'J值',
    #                     'MACD_12_26_9': 'MACD',
    #                     'MACDh_12_26_9': 'MACD柱',
    #                     'MACDs_12_26_9': 'MACD信号',
    #                     'RSI_6': 'RSI6',
    #                     'RSI_12': 'RSI12',
    #                     'RSI_24': 'RSI24',
    #                     'BBL_20_2.0': '布林下轨',
    #                     'BBM_20_2.0': '布林中轨',
    #                     'BBU_20_2.0': '布林上轨',
    #                     'BBB_20_2.0': '布林带宽',
    #                     'BBP_20_2.0': '布林带百分比'
    #                 }
    #             }
    #         }
    #     }
    
    def __init__(self, stock_codes=None, use_file=False, stock_file='stock_list.txt', 
                 kline_type='daily', fq_type='forward', start_date=None, end_date=None, 
                 calc_indicators=True, *args, **kwargs):
        super(StockKlineSpider, self).__init__(*args, **kwargs)
        
        # 获取指定日期或当前日期
        self.current_date = datetime.strptime(end_date, "%Y%m%d") if end_date else datetime.now()
        self.current_time = self.current_date.strftime("%Y-%m-%d")
        
        # 从文件读取股票代码或使用传入的股票代码
        if use_file and use_file.lower() == 'true':
            try:
                with open(stock_file, 'r', encoding='utf-8') as f:
                    self.stock_codes = [line.strip() for line in f if line.strip()]
                if not self.stock_codes:
                    self.logger.warning(f"股票代码文件 {stock_file} 为空，使用默认股票代码")
                    self.stock_codes = ['sh603288', 'sz000858']
            except FileNotFoundError:
                self.logger.error(f"找不到股票代码文件 {stock_file}，使用默认股票代码")
                self.stock_codes = ['sh603288', 'sz000858']
        else:
            self.stock_codes = stock_codes.split(',') if stock_codes else ['sh603288', 'sz000858']
        
        self.kline_type = kline_type
        self.fq_type = fq_type
        
        # 设置默认时间范围为最近一年
        one_year_ago = self.current_date - timedelta(days=365)
        
        # 设置起始日期
        self.start_date = one_year_ago.strftime("%Y%m%d")
        # 设置结束日期
        self.end_date = end_date if end_date else self.current_date.strftime("%Y%m%d")
            
        self.calc_indicators = calc_indicators
        self.kline_data = {}  # 用于临时存储K线数据
        self.fundamental_map = self._load_fundamental_cache()
        
        # 添加信号输出文件的路径
        self.signal_file = f'kdj_signals_{self.current_date.strftime("%Y%m%d")}.txt'
        # 清空信号文件
        with open(self.signal_file, 'w', encoding='utf-8') as f:
            f.write(f"股票信号分析报告 - {self.current_time}\n")
            f.write("=" * 80 + "\n\n")
        
        # 初始化数据库连接
        self.conn = sqlite3.connect('stock_signals.db')
        self.cursor = self.conn.cursor()
        self.create_table()

    def _normalize_stock_code(self, code):
        if not code:
            return None
        code = str(code).strip()
        if code.startswith(('sh', 'sz', 'bj')) and len(code) >= 8:
            return code
        if len(code) == 6 and code.isdigit():
            if code.startswith(('60', '68')):
                return f"sh{code}"
            if code.startswith(('00', '30')):
                return f"sz{code}"
            if code.startswith(('83', '87', '92', '43', '82', '88')):
                return f"bj{code}"
        return code

    def _parse_float(self, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if text in ("", "-", "--"):
            return None
        text = text.replace("%", "").replace(",", "")
        try:
            return float(text)
        except ValueError:
            return None

    def _load_fundamental_cache(self):
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        candidates = [
            os.path.join(os.getcwd(), "stock_detail_data.csv"),
            os.path.join(os.getcwd(), "eastmoney_data.csv"),
            os.path.join(project_root, "stock_detail_data.csv"),
            os.path.join(project_root, "eastmoney_data.csv"),
        ]
        header_map = {
            '股票代码': 'stock_code',
            'stock_id': 'stock_code',
            '市盈率': 'pe',
            '市净率': 'pb',
            '换手率': 'turnover_rate',
            '成交额': 'trading_value',
            '成交量(手)': 'trading_volume'
        }
        fundamental_map = {}
        for path in candidates:
            if not os.path.exists(path):
                continue
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    headers = next(reader, None)
                    if not headers:
                        continue
                    indices = {header_map.get(h, h): i for i, h in enumerate(headers)}
                    for row in reader:
                        if not row:
                            continue
                        code_index = indices.get('stock_code')
                        raw_code = row[code_index] if code_index is not None and code_index < len(row) else None
                        stock_code = self._normalize_stock_code(raw_code)
                        if not stock_code:
                            continue
                        pe_index = indices.get('pe')
                        pb_index = indices.get('pb')
                        pe = self._parse_float(row[pe_index]) if pe_index is not None and pe_index < len(row) else None
                        pb = self._parse_float(row[pb_index]) if pb_index is not None and pb_index < len(row) else None
                        fundamental_map[stock_code] = {
                            'pe': pe,
                            'pb': pb
                        }
            except Exception as e:
                self.logger.warning(f"加载估值缓存失败: {path}, 错误: {str(e)}")
                continue
        if fundamental_map:
            self.logger.warning(f"估值缓存加载完成: {len(fundamental_map)} 只股票")
        return fundamental_map

    def _passes_valuation_filters(self, stock_code):
        if not SIGNAL_FILTERS['valuation'].get('enable', True):
            return True
        if not stock_code or not self.fundamental_map:
            return True
        data = self.fundamental_map.get(stock_code)
        if not data:
            return True
        pe = data.get('pe')
        pb = data.get('pb')
        pe_min = SIGNAL_FILTERS['valuation'].get('pe_min')
        pe_max = SIGNAL_FILTERS['valuation'].get('pe_max')
        pb_min = SIGNAL_FILTERS['valuation'].get('pb_min')
        pb_max = SIGNAL_FILTERS['valuation'].get('pb_max')
        if pe is not None and pe_min is not None and pe < pe_min:
            return False
        if pe is not None and pe_max is not None and pe > pe_max:
            return False
        if pb is not None and pb_min is not None and pb < pb_min:
            return False
        if pb is not None and pb_max is not None and pb > pb_max:
            return False
        return True

    def _check_valuation_filters(self, stock_code):
        if not SIGNAL_FILTERS['valuation'].get('enable', True):
            return True, 'disabled'
        if not stock_code or not self.fundamental_map:
            return True, 'missing'
        data = self.fundamental_map.get(stock_code)
        if not data:
            return True, 'missing'
        pe = data.get('pe')
        pb = data.get('pb')
        pe_min = SIGNAL_FILTERS['valuation'].get('pe_min')
        pe_max = SIGNAL_FILTERS['valuation'].get('pe_max')
        pb_min = SIGNAL_FILTERS['valuation'].get('pb_min')
        pb_max = SIGNAL_FILTERS['valuation'].get('pb_max')
        if pe is not None and pe_min is not None and pe < pe_min:
            return False, 'blocked'
        if pe is not None and pe_max is not None and pe > pe_max:
            return False, 'blocked'
        if pb is not None and pb_min is not None and pb < pb_min:
            return False, 'blocked'
        if pb is not None and pb_max is not None and pb > pb_max:
            return False, 'blocked'
        return True, 'passed'

    def _passes_liquidity_filters(self, df, pos, signal_type):
        liquidity_cfg = SIGNAL_FILTERS['liquidity']
        avg_days = liquidity_cfg.get('avg_days', 20)
        start = max(0, pos - avg_days + 1)
        window = df.iloc[start:pos + 1]
        if window.empty:
            return False

        avg_amount = None
        if 'amount' in window.columns:
            avg_amount = window['amount'].mean()
            if avg_amount is not None and avg_amount < liquidity_cfg.get('min_avg_amount', 0):
                return False

        avg_turnover = None
        if 'turnover' in window.columns:
            avg_turnover = window['turnover'].mean()
            if avg_turnover is not None and avg_turnover < liquidity_cfg.get('min_avg_turnover_rate', 0):
                return False

        if 'volume' in window.columns:
            avg_volume = window['volume'].mean()
            current_volume = df.iloc[pos]['volume']
            if avg_volume and current_volume is not None and avg_volume > 0:
                volume_ratio = current_volume / avg_volume
                trend_signals = {
                    'macd_golden_cross',
                    'macd_zero_cross',
                    'ma_golden_cross',
                    'dmi_golden_cross',
                    'dmi_adx_strong',
                    'boll_width_expand'
                }
                required_ratio = liquidity_cfg.get('trend_volume_ratio') if signal_type in trend_signals else liquidity_cfg.get('min_volume_ratio')
                if required_ratio and volume_ratio < required_ratio:
                    return False
        return True

    def _passes_trade_status(self, df, pos):
        if not SIGNAL_FILTERS.get('require_tradestatus', True):
            return True
        if 'trade_status' not in df.columns:
            return True
        value = df.iloc[pos].get('trade_status')
        if value in (0, '0'):
            return False
        return True

    def _is_st(self, df, pos):
        if not SIGNAL_FILTERS.get('exclude_st', True):
            return False
        if 'is_st' not in df.columns:
            return False
        value = df.iloc[pos].get('is_st')
        return value in (1, '1')

    def _compute_volume_heat_score(self, df):
        """
        最近交易热度评分 0–100（仅作参考，不参与过滤；每条分析结果在「总成功数」后输出一行）：
        - 量趋势（默认 40）：优先用过去约 120 日 r_trend 的**历史分位数**映射到满分；样本不足则退回固定区间线性映射。
        - 近 N 日放量（默认 25）：**近 3 日均量 / MA20**（非单日），并与 r_trend 做依赖约束：s_vol *= min(1, r_trend)。
        - 成交活跃（默认 35）：MA(短)成交额 / MA(长)成交额；无成交额列时用「近短均量/再前一段均量」近似。
        - 最后整体 × 流动性折扣：min(1, 近20日均成交额 / liquidity_floor)，冷门票不因异常放量虚高。
        """
        cfg = SIGNAL_FILTERS.get('volume_heat') or {}
        if not cfg.get('enable', True):
            return None, {}
        ma_s = int(cfg.get('ma_short', 5))
        ma_l = int(cfg.get('ma_long', 20))
        ma_vr = int(cfg.get('ma_vol_recent', 3))
        w = cfg.get('weights') or {}
        w_trend = float(w.get('trend', 40))
        w_vol = float(w.get('vol_recent', 25))
        w_amt = float(w.get('amount', 35))
        lookback = int(cfg.get('percentile_lookback', 120))
        min_pct_samples = int(cfg.get('percentile_min_samples', 30))
        use_pct = bool(cfg.get('use_percentile_trend', True))
        liq_floor = float(cfg.get('liquidity_floor', 1e8))

        if 'volume' not in df.columns or len(df) < ma_l + 1:
            return None, {}
        vol = pd.to_numeric(df['volume'], errors='coerce').fillna(0.0)
        last_idx = len(df) - 1
        start_l = max(0, last_idx - ma_l + 1)
        vol_ma_l = float(vol.iloc[start_l:last_idx + 1].mean())
        if vol_ma_l <= 0:
            return None, {}
        start_s = max(0, last_idx - ma_s + 1)
        vol_ma_s = float(vol.iloc[start_s:last_idx + 1].mean())
        r_trend = vol_ma_s / vol_ma_l

        def _r_trend_at(j):
            if j < ma_l - 1:
                return None
            ss = max(0, j - ma_s + 1)
            sl = max(0, j - ma_l + 1)
            vs = float(vol.iloc[ss:j + 1].mean())
            vl = float(vol.iloc[sl:j + 1].mean())
            if vl <= 0:
                return None
            return vs / vl

        def _lin_map(x, lo, hi, out_max):
            if hi <= lo:
                return 0.0
            t = (x - lo) / (hi - lo)
            t = max(0.0, min(1.0, t))
            return round(t * out_max, 2)

        # 量趋势：分位数映射到 w_trend
        hist_trend = []
        if use_pct and last_idx >= ma_l - 1:
            j0 = max(ma_l - 1, last_idx - lookback + 1)
            for j in range(j0, last_idx + 1):
                rt = _r_trend_at(j)
                if rt is not None:
                    hist_trend.append(rt)
        if use_pct and len(hist_trend) >= min_pct_samples:
            sh = sorted(hist_trend)
            rank_frac = bisect.bisect_right(sh, r_trend) / len(sh)
            s_trend = round(rank_frac * w_trend, 2)
            trend_mode = 'percentile'
        else:
            s_trend = _lin_map(r_trend, 0.75, 1.25, w_trend)
            trend_mode = 'linear_fallback'

        # 近 ma_vr 日均量 / MA20，降噪单日拉爆
        vr_start = max(0, last_idx - ma_vr + 1)
        vol_ma_recent = float(vol.iloc[vr_start:last_idx + 1].mean())
        r_vol_recent = vol_ma_recent / vol_ma_l
        s_vol_raw = _lin_map(r_vol_recent, 0.8, 1.6, w_vol)
        dep = min(1.0, r_trend)
        s_vol = round(s_vol_raw * dep, 2)

        s_amt = 0.0
        r_amt = None
        if 'amount' in df.columns:
            amt = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
            amt_ma_l = float(amt.iloc[start_l:last_idx + 1].mean())
            amt_ma_s = float(amt.iloc[start_s:last_idx + 1].mean())
            if amt_ma_l > 0:
                r_amt = amt_ma_s / amt_ma_l
                s_amt = _lin_map(r_amt, 0.75, 1.25, w_amt)
        else:
            if last_idx >= ma_s + ma_l:
                prev_start = max(0, last_idx - ma_s - ma_l + 1)
                prev_end = last_idx - ma_s + 1
                vol_prev = float(vol.iloc[prev_start:prev_end].mean())
                if vol_prev > 0:
                    r_amt = vol_ma_s / vol_prev
                    s_amt = _lin_map(r_amt, 0.85, 1.35, w_amt)

        total_base = s_trend + s_vol + s_amt
        liq_factor = 1.0
        if 'amount' in df.columns and liq_floor > 0:
            amt = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
            avg_amt_20 = float(amt.iloc[start_l:last_idx + 1].mean())
            liq_factor = min(1.0, avg_amt_20 / liq_floor)

        total = round(min(100.0, total_base * liq_factor), 1)
        detail = {
            'vol_ma_ratio_5_20': round(r_trend, 3),
            'volume_ratio_3d_ma20': round(r_vol_recent, 3),
            'amount_ma_ratio_5_20': round(r_amt, 3) if r_amt is not None else None,
            'score_trend': s_trend,
            'score_vol_recent': s_vol,
            'score_vol_recent_raw': s_vol_raw,
            'trend_dependency': round(dep, 3),
            'score_amount': s_amt,
            'trend_mode': trend_mode,
            'liquidity_discount': round(liq_factor, 3),
            'raw_total_before_liq': round(total_base, 2),
        }
        return total, detail

    def _recent_trade_heat_line(self, total):
        """每条股票分析结果仅一行：最近交易热度评分（紧跟「总成功数」后）。"""
        if total is None:
            return None
        return f"最近交易热度评分: {total}/100"

    def create_table(self):
        """创建数据库表"""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                date TEXT,
                signal TEXT,
                success_rate REAL,
                initial_price REAL,
                created_at TEXT,
                UNIQUE(stock_code, date, signal)
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                signal TEXT,
                signal_count INTEGER,
                overall_success_rate REAL,
                insert_date TEXT,
                insert_price REAL,
                highest_price REAL,
                highest_price_date TEXT,
                highest_change_rate REAL,
                highest_days INTEGER,
                lowest_price REAL,
                lowest_price_date TEXT,
                lowest_change_rate REAL,
                lowest_days INTEGER,
                buy_day_change_rate REAL,
                next_day_change_rate REAL,
                created_at TEXT
            )
        ''')
        try:
            self.cursor.execute('ALTER TABLE stock_signals ADD COLUMN buy_day_change_rate REAL')
        except:
            pass
        try:
            self.cursor.execute('ALTER TABLE stock_signals ADD COLUMN next_day_change_rate REAL')
        except:
            pass
        
        # 创建每日价格数据表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_signal_daily_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER NOT NULL,
                stock_code TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                days_from_signal INTEGER NOT NULL,
                created_at TEXT,
                FOREIGN KEY (signal_id) REFERENCES stock_signals(id),
                UNIQUE(signal_id, date)
            )
        ''')
        
        # 创建索引提升查询性能
        try:
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_signal_daily_prices_signal_id ON stock_signal_daily_prices(signal_id)')
        except:
            pass
        try:
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_signal_daily_prices_stock_code ON stock_signal_daily_prices(stock_code)')
        except:
            pass
        try:
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_signal_daily_prices_date ON stock_signal_daily_prices(date)')
        except:
            pass
        
        self.conn.commit()
    
    def start_requests(self):
        # 根据数据源配置选择不同的获取方式
        if DATA_SOURCE == 'baostock':
            # 多进程并行：每个进程独立连接 baostock，互不干扰，可真正并行
            workers = max(1, min(int(BAOSTOCK_FETCH_WORKERS), 16))
            total = len(self.stock_codes)
            self.logger.warning(f"开始拉取 {total} 只股票，{workers} 进程并行，每 50 只打印进度")
            results = {}
            done = 0
            try:
                with ProcessPoolExecutor(max_workers=workers) as executor:
                    futures = {
                        executor.submit(
                            fetch_one_baostock_worker,
                            code,
                            self.start_date,
                            self.end_date,
                        ): code
                        for code in self.stock_codes
                    }
                    for future in as_completed(futures):
                        code = futures[future]
                        try:
                            results[code] = future.result()
                        except Exception as e:
                            self.logger.error(f"拉取 {code} 出错: {e}")
                            results[code] = (code, None, None)
                        done += 1
                        if done == 1 or done % 50 == 0 or done == total:
                            self.logger.warning(f"已拉取 {done}/{total} 只")
            except PermissionError as e:
                self.logger.warning(f"进程池不可用，改为串行拉取: {e}")
                results = {}
                done = 0
                for code in self.stock_codes:
                    try:
                        results[code] = fetch_one_baostock_worker(code, self.start_date, self.end_date)
                    except Exception as e2:
                        self.logger.error(f"拉取 {code} 出错: {e2}")
                        results[code] = (code, None, None)
                    done += 1
                    if done == 1 or done % 50 == 0 or done == total:
                        self.logger.warning(f"已拉取 {done}/{total} 只")

            for count, stock_code in enumerate(self.stock_codes, 1):
                if stock_code not in results:
                    continue
                stock_code, stock_name, df = results[stock_code]
                if df is not None and not df.empty:
                    self.logger.warning(f"开始处理第{count}个股票 {stock_code} 的数据")
                    self.process_kline_data(stock_code, stock_name, df)
                else:
                    self.logger.error(f"无法获取 {stock_code} 的K线数据")
            return

        # 非 baostock：原有逐只请求逻辑（东方财富等）
        # 东方财富 API（Scrapy Request 机制）
        count = 0
        for stock_code in self.stock_codes:
            count += 1
            self.logger.warning(f"开始请求第{count}个股票 {stock_code} 的数据")
            prefix = STOCK_PREFIX_MAP.get(stock_code[:2])
            if not prefix:
                self.logger.error(f"不支持的股票代码前缀: {stock_code}")
                continue
            params = {
                'secid': f"{prefix}.{stock_code[2:]}",
                'fields1': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13',
                'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
                'klt': KLINE_API['klt'][self.kline_type],
                'fqt': KLINE_API['fqt'][self.fq_type],
                'ut': KLINE_API['ut'],
                'beg': self.start_date or '',
                'end': self.end_date or '',
                'lmt': '1000',
            }
            url = f"{KLINE_API['base_url']}?" + "&".join([f"{k}={v}" for k, v in params.items()])
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={'stock_code': stock_code},
                headers=HEADERS
            )
    
    def write_to_signal_file(self, content):
        """将内容写入信号文件"""
        with open(self.signal_file, 'a', encoding='utf-8') as f:
            f.write(f"{content}\n")
        # 同时保存到数据库
        # self.save_to_database(content)
    
    def save_to_database(self, content):
        """将信号保存到数据库"""
        try:
            # 解析content并插入到数据库
            lines = content.split('\n')
            for line in lines:
                if "股票:" in line:
                    try:
                        parts = line.split(',')
                        # 解析股票信息
                        stock_part = parts[0].split('股票:')[1].strip()
                        # 提取股票名称和代码
                        if '(' in stock_part and ')' in stock_part:
                            stock_name = stock_part[:stock_part.find('(')].strip()
                            stock_code = stock_part[stock_part.find('(')+1:stock_part.find(')')].strip()
                        else:
                            continue  # 如果格式不正确跳过这条记录
                        
                        # 解析其他信息
                        date_str = next((p.split(': ')[1].strip() for p in parts if '日期:' in p), None)
                        # 统一日期格式为YYYY-MM-DD
                        if date_str:
                            try:
                                date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
                            except ValueError:
                                try:
                                    date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
                                except ValueError:
                                    self.logger.error(f"无法解析日期格式: {date_str}")
                                    continue
                        else:
                            continue
                            
                        signal = next((p.split(': ')[1].strip() for p in parts if '信号:' in p), None)
                        
                        # 特殊处理信号胜率
                        success_rate_part = next((p for p in parts if '信号胜率:' in p), None)
                        if success_rate_part:
                            success_rate_str = success_rate_part.split('信号胜率:')[1].strip()
                            success_rate = float(success_rate_str.split('%')[0].strip())
                        else:
                            success_rate = None
                            
                        initial_price = next((float(p.split(': ')[1].strip()) for p in parts if '收盘价:' in p), None)

                        # 只有当所有必要信息都存在时才插入数据库
                        if all([stock_code, stock_name, date, signal, success_rate, initial_price]):
                            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            # 检查是否已存在相同记录
                            self.cursor.execute('''
                                SELECT COUNT(*) FROM stock_data 
                                WHERE stock_code=? AND date=? AND signal=?
                            ''', (stock_code, date, signal))
                            
                            if self.cursor.fetchone()[0] == 0:
                                self.cursor.execute('''
                                    INSERT INTO stock_data (
                                        stock_code, stock_name, date, signal, 
                                        success_rate, initial_price, created_at
                                    )
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    stock_code,
                                    stock_name,
                                    signal['date'].strftime("%Y-%m-%d"),
                                    signal['signal'],
                                    round(signal['signal_success_rate'], 2),
                                    round(signal['close'], 2),
                                    current_time
                                ))
                            
                    except (IndexError, ValueError) as e:
                        self.logger.error(f"▲ 解析信号行时出错: {line}")
                        self.logger.error(str(e))
                        continue  # 跳过这条记录，继续处理下一条
            
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"▲ 保存到数据库时出错: {str(e)}")
            self.conn.rollback()  # 发生错误时回滚事务
    
    def parse(self, response):
        try:
            data = json.loads(response.text)
            if data.get('data') and data['data'].get('klines'):
                stock_code = response.meta['stock_code']
                klines = data['data']['klines']
                                
                # 检查数据量是否足够
                if len(klines) < 16:
                    self.logger.warning(f"票 {stock_code} 的数据量不足16天，跳过分析")
                    return
                
                # 将K线数据转换为DataFrame
                kline_data = []
                for kline in klines:
                    values = kline.split(',')
                    item = {}
                    for i, value in enumerate(values):
                        field = KLINE_FIELD_MAPPING.get(i)
                        if field:
                            if field != 'date':
                                try:
                                    item[field] = float(value)
                                except ValueError:
                                    item[field] = None
                            else:
                                item[field] = value
                    kline_data.append(item)
                
                # 创建DataFrame
                df = pd.DataFrame(kline_data)
                df.set_index('date', inplace=True)
                
                last_close_price = df.iloc[-1]['close']  # 最近的收盘价
                self.logger.info(f"最近一天的日期: {df.iloc[-1].name}, 收盘价: {last_close_price}")

                # 算技术指标
                if self.calc_indicators:
                    df = TechnicalIndicators.calculate_all(df, INDICATORS_CONFIG)
                    
                    # 分析信号
                    kdj_analysis = self.analyze_signals(df, stock_code=stock_code)
                    
                    # 只要有满足条件的信号写入文件
                    if kdj_analysis['recent_signals']:
                        # 统计信号种类和数量
                        signal_type_count = {}
                        for signal in kdj_analysis['recent_signals']:
                            signal_type = signal['signal']
                            signal_type_count[signal_type] = signal_type_count.get(signal_type, 0) + 1
                        
                        # 有当出现六种以上不同信号时才输出
                        if len(signal_type_count) > 5:
                            # 写入文件
                            self.write_to_signal_file(f"\n股票 {data['data']['name']}({stock_code}) 股票信号分析结果")
                            self.write_to_signal_file(f"总体成功率: {kdj_analysis['overall_success_rate']:.2f}%")
                            self.write_to_signal_file(f"总信号数: {kdj_analysis['total_signals']}")
                            self.write_to_signal_file(f"总成功数: {kdj_analysis['total_success']}")
                            vh, _vhd = self._compute_volume_heat_score(df)
                            heat_line = self._recent_trade_heat_line(vh)
                            if heat_line:
                                self.write_to_signal_file(heat_line)
                            
                            # 输出最近信号
                            self.write_to_signal_file("\n最近3天出现的高胜率信号：")
                            self.write_to_signal_file(f"共有{len(kdj_analysis['recent_signals'])}个信号，{len(signal_type_count)}种类型：")
                            # 输出每种信号的数量
                            for signal_type, count in signal_type_count.items():
                                self.write_to_signal_file(f"- {signal_type}: {count}个")
                            
                            # 批量处理数据库插入
                            signals_to_insert = []
                            for signal in kdj_analysis['recent_signals']:
                                signals_to_insert.append((
                                    stock_code,
                                    data['data']['name'],
                                    signal['date'].strftime("%Y-%m-%d"),
                                    signal['signal'],
                                    round(signal['signal_success_rate'], 2),
                                    round(signal['close'], 2),
                                    self.current_time
                                ))

                            if signals_to_insert:
                                # 使用executemany一次性插入多条记录（带唯一约束与 OR IGNORE，重复不会插入）
                                self.cursor.executemany('''
                                    INSERT OR IGNORE INTO stock_data (
                                        stock_code, stock_name, date, signal, 
                                        success_rate, initial_price, created_at
                                    )
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                ''', signals_to_insert)
                                self.conn.commit()
                            
                            # 为保证同一股票在同一天只有一条汇总记录，
                            # 在插入当日 stock_signals 之前，先删除同一股票、同一 insert_date 的旧记录
                            self.cursor.execute('''
                                DELETE FROM stock_signals
                                WHERE stock_code = ? AND insert_date = ?
                            ''', (stock_code, self.current_time))
                            
                            self.cursor.execute('''
                                INSERT INTO stock_signals (
                                    stock_code, stock_name, signal, signal_count,
                                    overall_success_rate, insert_date, insert_price,
                                    created_at
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                stock_code,
                                data['data']['name'],
                                ','.join(signal_type_count.keys()),
                                len(signal_type_count),
                                round(kdj_analysis['overall_success_rate'], 2),
                                self.current_time,
                                round(last_close_price, 2),
                                self.current_time
                            ))
                            
                            # 输出信号到文件
                            for signal in kdj_analysis['recent_signals']:
                                # 输出信号相关信息
                                if signal:
                                    signal_info = []
                                    
                                    # 基础信息
                                    signal_info.extend([
                                        f"日期: {signal['date'].strftime('%Y-%m-%d')}",
                                        f"信号类型: {signal['signal_type']}",
                                        f"信号: {signal['signal']}",
                                        f"信号胜率: {signal['signal_success_rate']:.2f}%",
                                        f"(历史出现: {signal['signal_total']}次)",
                                        f"整体胜率: {signal['overall_success_rate']:.2f}%",
                                        f"收盘价: {signal['close']:.2f}"
                                    ])
                                    
                                    # 根据信号类型添加对应的指标信息
                                    if signal['signal_type'].startswith('kdj'):
                                        signal_info.extend([
                                            f"K值: {signal.get('k_value', 'N/A'):.2f}",
                                            f"D值: {signal.get('d_value', 'N/A'):.2f}",
                                            f"J值: {signal.get('j_value', 'N/A'):.2f}"
                                        ])
                                    elif signal['signal_type'].startswith('macd'):
                                        signal_info.extend([
                                            f"MACD: {signal.get('macd', 'N/A'):.4f}",
                                            f"MACD信号: {signal.get('macd_signal', 'N/A'):.4f}"
                                        ])
                                    elif signal['signal_type'].startswith('rsi'):
                                        signal_info.extend([
                                            f"RSI(6): {signal.get('RSI_6', 'N/A'):.2f}",
                                            f"RSI(12): {signal.get('RSI_12', 'N/A'):.2f}"
                                        ])
                                    elif signal['signal_type'].startswith('boll'):
                                        signal_info.extend([
                                            f"布林下轨: {signal.get('BBL_20_2.0', 'N/A'):.2f}",
                                            f"布林中轨: {signal.get('BBM_20_2.0', 'N/A'):.2f}",
                                            f"布林上轨: {signal.get('BBU_20_2.0', 'N/A'):.2f}"
                                        ])
                                    elif signal['signal_type'].startswith('ma'):
                                        signal_info.extend([
                                            f"MA5: {signal.get('SMA_5', 'N/A'):.2f}",
                                            f"MA20: {signal.get('SMA_20', 'N/A'):.2f}"
                                        ])
                                    elif signal['signal_type'].startswith('dmi'):
                                        signal_info.extend([
                                            f"DMP(14): {signal.get('DMP_14', 'N/A'):.2f}",
                                            f"DMN(14): {signal.get('DMN_14', 'N/A'):.2f}",
                                            f"ADX(14): {signal.get('ADX_14', 'N/A'):.2f}"
                                        ])
                                    elif signal['signal_type'].startswith('cci'):
                                        signal_info.extend([
                                            f"CCI(20): {signal.get('CCI_20', 'N/A'):.2f}"
                                        ])
                                    elif signal['signal_type'].startswith('roc'):
                                        signal_info.extend([
                                            f"ROC(12): {signal.get('ROC_12', 'N/A'):.2f}"
                                        ])
                                    
                                    # 将所有信息用逗号连接并输出
                                    signal_info_str = ", ".join(signal_info)
                                    self.logger.info(signal_info_str)
                                    # 同时写入信号文件
                                    self.write_to_signal_file(f"股票: {data['data']['name']}({stock_code}), {signal_info_str}")
                            self.write_to_signal_file("-" * 80)  # 分隔线
                            
                            # 同时保持控制台输出
                            self.logger.warning(f"股票 {stock_code} KDJ信号分析结果已写入文件: {self.signal_file}")
                        else:
                            self.logger.warning(f"股票 {stock_code} 最近3天的信号类型数量 {len(signal_type_count)}，跳过输出")
                    else:
                        self.logger.info(f"股票 {stock_code} 最近3天没有满足条件的高胜信号")
                
                # 结果数据
                for index, row in df.iterrows():
                    item = dict(row)
                    item.update({
                        'stock_code': stock_code,
                        'date': index,
                        'type': self.kline_type,
                        'fq_type': self.fq_type
                    })
                    
                    # print(f"获取到K线数据: {stock_code} - {index}")
                    yield item
                    
            else:
                self.logger.error(f"未获取到股票 {response.meta['stock_code']} 的K线数据")
                
        except Exception as e:
            error_msg = f"解析股票 {response.meta['stock_code']} 的K线数据出错: {str(e)}"
            self.logger.error(error_msg)
            self.write_to_signal_file(f"\n错误: {error_msg}")
            import traceback
            self.write_to_signal_file(traceback.format_exc())
            return  # 出现异常时直接返回，不继续执行
        
    def process_kline_data(self, stock_code, stock_name, df):
        """
        处理K线数据（用于baostock数据源）
        
        参数:
            stock_code: 股票代码
            stock_name: 股票名称
            df: pandas.DataFrame，包含K线数据
        """
        try:
            # 检查数据量是否足够
            min_history_days = SIGNAL_FILTERS.get('min_history_days', 60)
            if len(df) < min_history_days:
                self.logger.warning(f"股票 {stock_code} 的数据量不足{min_history_days}天，跳过分析")
                return
            
            # 确保日期索引是datetime类型
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            
            last_close_price = df.iloc[-1]['close']  # 最近的收盘价
            self.logger.info(f"最近一天的日期: {df.iloc[-1].name}, 收盘价: {last_close_price}")

            # 算技术指标
            if self.calc_indicators:
                df = TechnicalIndicators.calculate_all(df, INDICATORS_CONFIG)
                
                # 分析信号
                kdj_analysis = self.analyze_signals(df, stock_code=stock_code)
                
                # 只要有满足条件的信号写入文件
                if kdj_analysis['recent_signals']:
                    # 统计信号种类和数量
                    signal_type_count = {}
                    for signal in kdj_analysis['recent_signals']:
                        signal_type = signal['signal']
                        signal_type_count[signal_type] = signal_type_count.get(signal_type, 0) + 1
                    
                    # 有当出现六种以上不同信号时才输出
                    if len(signal_type_count) > 5:
                        # 写入文件
                        self.write_to_signal_file(f"\n股票 {stock_name}({stock_code}) 股票信号分析结果")
                        self.write_to_signal_file(f"总体成功率: {kdj_analysis['overall_success_rate']:.2f}%")
                        self.write_to_signal_file(f"总信号数: {kdj_analysis['total_signals']}")
                        self.write_to_signal_file(f"总成功数: {kdj_analysis['total_success']}")
                        vh, _vhd = self._compute_volume_heat_score(df)
                        heat_line = self._recent_trade_heat_line(vh)
                        if heat_line:
                            self.write_to_signal_file(heat_line)
                        
                        # 输出最近信号
                        self.write_to_signal_file("\n最近3天出现的高胜率信号：")
                        self.write_to_signal_file(f"共有{len(kdj_analysis['recent_signals'])}个信号，{len(signal_type_count)}种类型：")
                        # 输出每种信号的数量
                        for signal_type, count in signal_type_count.items():
                            self.write_to_signal_file(f"- {signal_type}: {count}个")
                        
                        # 批量处理数据库插入
                        signals_to_insert = []
                        for signal in kdj_analysis['recent_signals']:
                            signals_to_insert.append((
                                stock_code,
                                stock_name,
                                signal['date'].strftime("%Y-%m-%d"),
                                signal['signal'],
                                round(signal['signal_success_rate'], 2),
                                round(signal['close'], 2),
                                self.current_time
                            ))

                        if signals_to_insert:
                            # 使用executemany一次性插入多条记录（带唯一约束与 OR IGNORE，重复不会插入）
                            self.cursor.executemany('''
                                INSERT OR IGNORE INTO stock_data (
                                    stock_code, stock_name, date, signal, 
                                    success_rate, initial_price, created_at
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', signals_to_insert)
                            self.conn.commit()
                        
                        # 为保证同一股票在同一天只有一条汇总记录，
                        # 在插入当日 stock_signals 之前，先删除同一股票、同一 insert_date 的旧记录
                        self.cursor.execute('''
                            DELETE FROM stock_signals
                            WHERE stock_code = ? AND insert_date = ?
                        ''', (stock_code, self.current_time))
                        
                        self.cursor.execute('''
                            INSERT INTO stock_signals (
                                stock_code, stock_name, signal, signal_count,
                                overall_success_rate, insert_date, insert_price,
                                created_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            stock_code,
                            stock_name,
                            ','.join(signal_type_count.keys()),
                            len(signal_type_count),
                            round(kdj_analysis['overall_success_rate'], 2),
                            self.current_time,
                            round(last_close_price, 2),
                            self.current_time
                        ))
                        
                        # 输出信号到文件
                        for signal in kdj_analysis['recent_signals']:
                            # 输出信号相关信息
                            if signal:
                                signal_info = []
                                
                                # 基础信息
                                signal_info.extend([
                                    f"日期: {signal['date'].strftime('%Y-%m-%d')}",
                                    f"信号类型: {signal['signal_type']}",
                                    f"信号: {signal['signal']}",
                                    f"信号胜率: {signal['signal_success_rate']:.2f}%",
                                    f"(历史出现: {signal['signal_total']}次)",
                                    f"整体胜率: {signal['overall_success_rate']:.2f}%",
                                    f"收盘价: {signal['close']:.2f}"
                                ])
                                
                                # 根据信号类型添加对应的指标信息
                                if signal['signal_type'].startswith('kdj'):
                                    signal_info.extend([
                                        f"K值: {signal.get('k_value', 'N/A'):.2f}",
                                        f"D值: {signal.get('d_value', 'N/A'):.2f}",
                                        f"J值: {signal.get('j_value', 'N/A'):.2f}"
                                    ])
                                elif signal['signal_type'].startswith('macd'):
                                    signal_info.extend([
                                        f"MACD: {signal.get('macd', 'N/A'):.4f}",
                                        f"MACD信号: {signal.get('macd_signal', 'N/A'):.4f}"
                                    ])
                                elif signal['signal_type'].startswith('rsi'):
                                    signal_info.extend([
                                        f"RSI(6): {signal.get('RSI_6', 'N/A'):.2f}",
                                        f"RSI(12): {signal.get('RSI_12', 'N/A'):.2f}"
                                    ])
                                elif signal['signal_type'].startswith('boll'):
                                    signal_info.extend([
                                        f"布林下轨: {signal.get('BBL_20_2.0', 'N/A'):.2f}",
                                        f"布林中轨: {signal.get('BBM_20_2.0', 'N/A'):.2f}",
                                        f"布林上轨: {signal.get('BBU_20_2.0', 'N/A'):.2f}"
                                    ])
                                elif signal['signal_type'].startswith('ma'):
                                    signal_info.extend([
                                        f"MA5: {signal.get('SMA_5', 'N/A'):.2f}",
                                        f"MA20: {signal.get('SMA_20', 'N/A'):.2f}"
                                    ])
                                elif signal['signal_type'].startswith('dmi'):
                                    signal_info.extend([
                                        f"DMP(14): {signal.get('DMP_14', 'N/A'):.2f}",
                                        f"DMN(14): {signal.get('DMN_14', 'N/A'):.2f}",
                                        f"ADX(14): {signal.get('ADX_14', 'N/A'):.2f}"
                                    ])
                                elif signal['signal_type'].startswith('cci'):
                                    signal_info.extend([
                                        f"CCI(20): {signal.get('CCI_20', 'N/A'):.2f}"
                                    ])
                                elif signal['signal_type'].startswith('roc'):
                                    signal_info.extend([
                                        f"ROC(12): {signal.get('ROC_12', 'N/A'):.2f}"
                                    ])
                                
                                # 将所有信息用逗号连接并输出
                                signal_info_str = ", ".join(signal_info)
                                self.logger.info(signal_info_str)
                                # 同时写入信号文件
                                self.write_to_signal_file(f"股票: {stock_name}({stock_code}), {signal_info_str}")
                        self.write_to_signal_file("-" * 80)  # 分隔线
                        
                        # 同时保持控制台输出
                        self.logger.warning(f"股票 {stock_code} KDJ信号分析结果已写入文件: {self.signal_file}")
                    else:
                        self.logger.warning(f"股票 {stock_code} 最近3天的信号类型数量 {len(signal_type_count)}，跳过输出")
                else:
                    self.logger.info(f"股票 {stock_code} 最近3天没有满足条件的高胜信号")
            
            # 更新数据库中的最高价格
            self.update_price_extremes(stock_code, stock_name, df)
            
        except Exception as e:
            error_msg = f"处理股票 {stock_code} 的K线数据出错: {str(e)}"
            self.logger.error(error_msg)
            self.write_to_signal_file(f"\n错误: {error_msg}")
            import traceback
            self.write_to_signal_file(traceback.format_exc())
    
    def update_price_extremes(self, stock_code, stock_name, df):
        """更新数据库中记录的股票在日志记录时间30天内的最高和最低价格"""
        try:
            # 检查数据库中是否存在该股票的记录，只获取必要字段
            self.cursor.execute('''
                SELECT id, insert_price, insert_date
                FROM stock_signals 
                WHERE stock_code=? AND insert_date>=?
            ''', (stock_code, (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")))
            
            records = self.cursor.fetchall()
            if records:
                if not df.empty:
                    # 遍历所有记录，统计30天内的最高和最低价格
                    for record in records:
                        record_id, insert_price, insert_date = record
                        
                        # 如果insert_price为None，跳过这条记录
                        if insert_price is None:
                            self.logger.warning(f"记录ID {record_id} 的insert_price为None，跳过更新")
                            continue
                            
                        try:
                            # 将insert_date转换为日期格式
                            try:
                                # 先尝试转换完整的日期时间格式
                                insert_date = datetime.strptime(insert_date, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
                            except ValueError:
                                # 如果失败，尝试只转换日期部分
                                insert_date = datetime.strptime(insert_date, "%Y-%m-%d").strftime("%Y-%m-%d")
                            
                            # 确保DataFrame的索引是datetime类型
                            if not isinstance(df.index, pd.DatetimeIndex):
                                df.index = pd.to_datetime(df.index)
                            
                            try:
                                # 找到最接近的交易日
                                insert_date = pd.to_datetime(insert_date)
                                nearest_date = df.index[df.index >= insert_date][0]
                                created_idx = df.index.get_loc(nearest_date)
                                
                                # 获取从nearest_date当天到后续30天的数据（包含当天）
                                future_data = df.iloc[created_idx:created_idx + 31]  # 包含当天，所以不需要+1
                                
                                if not future_data.empty:
                                    # 确保close列中没有None值
                                    future_data = future_data[future_data['close'].notna()]
                                    
                                    if not future_data.empty:
                                        buy_day_change_rate = None
                                        next_day_change_rate = None
                                        
                                        buy_day_data = future_data.iloc[0:1]
                                        if not buy_day_data.empty and 'change_rate' in buy_day_data.columns:
                                            buy_day_change_rate = round(buy_day_data['change_rate'].iloc[0], 2) if pd.notna(buy_day_data['change_rate'].iloc[0]) else None
                                        
                                        if len(future_data) > 1:
                                            next_day_data = future_data.iloc[1:2]
                                            if not next_day_data.empty and 'change_rate' in next_day_data.columns:
                                                next_day_change_rate = round(next_day_data['change_rate'].iloc[0], 2) if pd.notna(next_day_data['change_rate'].iloc[0]) else None
                                        
                                        highest_price = round(future_data['close'].max(), 2)
                                        highest_price_date = future_data['close'].idxmax().strftime("%Y-%m-%d")
                                        highest_change_rate = round(((highest_price - insert_price) / insert_price * 100), 2)
                                        highest_days = (pd.to_datetime(highest_price_date) - insert_date).days
                                        
                                        lowest_price = round(future_data['close'].min(), 2)
                                        lowest_price_date = future_data['close'].idxmin().strftime("%Y-%m-%d")
                                        lowest_change_rate = round(((lowest_price - insert_price) / insert_price * 100), 2)
                                        lowest_days = (pd.to_datetime(lowest_price_date) - insert_date).days
                                        
                                        # 保存每日价格数据到 stock_signal_daily_prices 表
                                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                        for idx, (date, row) in enumerate(future_data.iterrows()):
                                            days_from_signal = idx  # 0表示当天，1表示第二天，以此类推
                                            
                                            # 删除旧数据（如果存在），确保幂等性
                                            self.cursor.execute('''
                                                DELETE FROM stock_signal_daily_prices
                                                WHERE signal_id=? AND date=?
                                            ''', (record_id, date.strftime("%Y-%m-%d")))
                                            
                                            # 插入新数据
                                            self.cursor.execute('''
                                                INSERT INTO stock_signal_daily_prices (
                                                    signal_id, stock_code, date, open, high, low, close,
                                                    days_from_signal, created_at
                                                )
                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                            ''', (
                                                record_id,
                                                stock_code,
                                                date.strftime("%Y-%m-%d"),
                                                round(row.get('open', 0), 2) if pd.notna(row.get('open')) else None,
                                                round(row.get('high', 0), 2) if pd.notna(row.get('high')) else None,
                                                round(row.get('low', 0), 2) if pd.notna(row.get('low')) else None,
                                                round(row.get('close', 0), 2) if pd.notna(row.get('close')) else None,
                                                days_from_signal,
                                                current_time
                                            ))
                                        
                                        self.cursor.execute('''
                                            UPDATE stock_signals
                                            SET highest_price=?, 
                                                highest_price_date=?,
                                                highest_change_rate=?,
                                                highest_days=?,
                                                lowest_price=?,
                                                lowest_price_date=?,
                                                lowest_change_rate=?,
                                                lowest_days=?,
                                                buy_day_change_rate=?,
                                                next_day_change_rate=?
                                            WHERE id=?
                                        ''', (highest_price, highest_price_date, highest_change_rate, highest_days,
                                             lowest_price, lowest_price_date, lowest_change_rate, lowest_days,
                                             buy_day_change_rate, next_day_change_rate, record_id))
                            except IndexError:
                                self.logger.warning(f"记录ID {record_id} 没有找到对应的交易日数据")
                            except Exception as e:
                                self.logger.error(f"处理日期时出错: {insert_date}, 错误: {str(e)}")
                        except (KeyError, ValueError) as e:
                            self.logger.error(f"处理日期时出错: {insert_date}, 错误: {str(e)}")
                            continue
                
                self.conn.commit()
        except Exception as e:
            self.logger.error(f"更新价格极值时出错: {str(e)}")
            self.conn.rollback()
    
    def analyze_signals(self, df, stock_code=None):
        """分析多个技术指标的信号"""
        signals = []
        signal_stats = {
            # KDJ信号
            'kdj_oversold': {'success': 0, 'total': 0},
            'kdj_golden_cross': {'success': 0, 'total': 0},
            'kdj_divergence': {'success': 0, 'total': 0},
            # MACD信号
            'macd_golden_cross': {'success': 0, 'total': 0},
            'macd_zero_cross': {'success': 0, 'total': 0},
            'macd_divergence': {'success': 0, 'total': 0},
            # RSI信号
            'rsi_oversold': {'success': 0, 'total': 0},
            'rsi_golden_cross': {'success': 0, 'total': 0},
            # BOLL信号
            'boll_bottom_touch': {'success': 0, 'total': 0},
            'boll_width_expand': {'success': 0, 'total': 0},
            # MA信号
            'ma_golden_cross': {'success': 0, 'total': 0},  # 短期均线上穿长期均线
            'ma_support': {'success': 0, 'total': 0},       # 价格在均线支撑位反弹
            # DMI信号
            'dmi_golden_cross': {'success': 0, 'total': 0}, # DI+上穿DI-
            'dmi_adx_strong': {'success': 0, 'total': 0},   # ADX大于某个阈值，表示趋势强烈
            # CCI信号
            'cci_oversold': {'success': 0, 'total': 0},     # CCI超卖
            'cci_zero_cross': {'success': 0, 'total': 0},   # CCI上穿零轴
            # ROC信号
            'roc_zero_cross': {'success': 0, 'total': 0},   # ROC上穿零轴
            'roc_divergence': {'success': 0, 'total': 0}    # ROC底背离
        }
        
        # 确保数据按日期排序
        df = df.sort_index()
        
        min_history_days = SIGNAL_FILTERS.get('min_history_days', 60)
        success_window_days = SIGNAL_FILTERS.get('success_window_days', 14)
        # 检查数据量是否足够
        if len(df) < min_history_days:
            return {
                'signal_stats': {},
                'overall_success_rate': 0,
                'total_signals': 0,
                'total_success': 0,
                'signals': [],
                'recent_signals': []
            }
        
        for i in range(1, len(df) - success_window_days):
            current_row = df.iloc[i]
            prev_row = df.iloc[i-1]
            
            signals_for_day = []  # 存储当天的所有信号
            
            # KDJ信号判断
            if (current_row.get('K_9_3') is not None and current_row.get('D_9_3') is not None and
                current_row['K_9_3'] < 20 and current_row['D_9_3'] < 20):
                signals_for_day.append(('KDJ超卖', 'kdj_oversold'))
            if (prev_row.get('K_9_3') is not None and prev_row.get('D_9_3') is not None and
                current_row.get('K_9_3') is not None and current_row.get('D_9_3') is not None and
                prev_row['K_9_3'] < prev_row['D_9_3'] and 
                current_row['K_9_3'] > current_row['D_9_3']):
                signals_for_day.append(('KDJ金叉', 'kdj_golden_cross'))
            if (current_row.get('K_9_3') is not None and
                current_row['close'] < df.iloc[i-5:i]['close'].min() and 
                current_row['K_9_3'] > df.iloc[i-5:i]['K_9_3'].min()):
                signals_for_day.append(('KDJ底背离', 'kdj_divergence'))
                
            # MACD信号判断
            if (prev_row.get('MACD_12_26_9') is not None and prev_row.get('MACDs_12_26_9') is not None and
                current_row.get('MACD_12_26_9') is not None and current_row.get('MACDs_12_26_9') is not None and
                prev_row['MACD_12_26_9'] < prev_row['MACDs_12_26_9'] and 
                current_row['MACD_12_26_9'] > current_row['MACDs_12_26_9']):
                signals_for_day.append(('MACD金叉', 'macd_golden_cross'))
            if (prev_row.get('MACD_12_26_9') is not None and 
                prev_row['MACD_12_26_9'] < 0 and 
                current_row.get('MACD_12_26_9') is not None and
                current_row['MACD_12_26_9'] > 0):
                signals_for_day.append(('MACD零轴上穿', 'macd_zero_cross'))
            if (current_row.get('MACD_12_26_9') is not None and
                current_row['close'] < df.iloc[i-5:i]['close'].min() and 
                current_row['MACD_12_26_9'] > df.iloc[i-5:i]['MACD_12_26_9'].min()):
                signals_for_day.append(('MACD底背离', 'macd_divergence'))
                
            # RSI信号判断
            if (current_row.get('RSI_6') is not None and current_row['RSI_6'] < 20):
                signals_for_day.append(('RSI超卖', 'rsi_oversold'))
            if (prev_row.get('RSI_6') is not None and prev_row.get('RSI_12') is not None and
                current_row.get('RSI_6') is not None and current_row.get('RSI_12') is not None and
                prev_row['RSI_6'] < prev_row['RSI_12'] and 
                current_row['RSI_6'] > current_row['RSI_12']):
                signals_for_day.append(('RSI金叉', 'rsi_golden_cross'))
                
            # BOLL信号判断
            if (current_row.get('BBL_20_2.0') is not None and
                current_row['close'] <= current_row['BBL_20_2.0'] * 1.01):
                signals_for_day.append(('BOLL下轨支撑', 'boll_bottom_touch'))
            if (current_row.get('BBB_20_2.0') is not None and prev_row.get('BBB_20_2.0') is not None and
                current_row['BBB_20_2.0'] > prev_row['BBB_20_2.0'] * 1.1):
                signals_for_day.append(('BOLL带宽扩张', 'boll_width_expand'))
            
            # MA信号判断
            if (prev_row.get('SMA_5') is not None and prev_row.get('SMA_20') is not None and
                current_row.get('SMA_5') is not None and current_row.get('SMA_20') is not None and
                prev_row['SMA_5'] < prev_row['SMA_20'] and 
                current_row['SMA_5'] > current_row['SMA_20']):
                signals_for_day.append(('MA5上穿MA20', 'ma_golden_cross'))
            if (current_row.get('SMA_20') is not None and
                current_row['close'] > current_row['SMA_20'] * 0.99 and 
                current_row['close'] < current_row['SMA_20'] * 1.01):
                signals_for_day.append(('MA20支撑', 'ma_support'))
            
            # DMI信号判断
            if (prev_row.get('DMP_14') is not None and prev_row.get('DMN_14') is not None and
                current_row.get('DMP_14') is not None and current_row.get('DMN_14') is not None and
                current_row.get('ADX_14') is not None and
                prev_row['DMP_14'] < prev_row['DMN_14'] and 
                current_row['DMP_14'] > current_row['DMN_14'] and 
                current_row['ADX_14'] > 20):
                signals_for_day.append(('DMI金叉', 'dmi_golden_cross'))
            if (current_row.get('ADX_14') is not None and current_row['ADX_14'] > 30):
                signals_for_day.append(('ADX强势', 'dmi_adx_strong'))
            
            # CCI信号判断
            if (current_row.get('CCI_20') is not None and current_row['CCI_20'] < -100):
                signals_for_day.append(('CCI超卖', 'cci_oversold'))
            if (prev_row.get('CCI_20') is not None and current_row.get('CCI_20') is not None and
                prev_row['CCI_20'] < 0 and current_row['CCI_20'] > 0):
                signals_for_day.append(('CCI零轴上穿', 'cci_zero_cross'))
            
            # ROC信号判断
            if (prev_row.get('ROC_12') is not None and current_row.get('ROC_12') is not None and
                prev_row['ROC_12'] < 0 and current_row['ROC_12'] > 0):
                signals_for_day.append(('ROC零轴上穿', 'roc_zero_cross'))
            if (current_row.get('ROC_12') is not None and
                current_row['close'] < df.iloc[i-5:i]['close'].min() and 
                current_row['ROC_12'] > df.iloc[i-5:i]['ROC_12'].min()):
                signals_for_day.append(('ROC底背离', 'roc_divergence'))

            # 处理当天的所有信号
            for signal, signal_type in signals_for_day:
                # 检查未来14天是否有5%以上涨幅
                future_prices = df.iloc[i+1:i+1+success_window_days]['close']
                if len(future_prices) < success_window_days or future_prices.isna().all():
                    max_future_return = None
                    success = None
                else:
                    max_future_return = round(((future_prices.max() - current_row['close']) /
                                       current_row['close'] * 100), 2)
                    success = max_future_return >= 5
                    signal_stats[signal_type]['total'] += 1
                    if success:
                        signal_stats[signal_type]['success'] += 1

                signals.append({
                    'date': pd.to_datetime(df.index, format='%Y-%m-%d'),  # 修改日期格式
                    'signal_type': signal_type,
                    'signal': signal,
                    'close': current_row['close'],
                    'k_value': current_row.get('K_9_3'),
                    'd_value': current_row.get('D_9_3'),
                    'j_value': current_row.get('J_9_3'),
                    'macd': current_row.get('MACD_12_26_9'),
                    'macd_signal': current_row.get('MACDs_12_26_9'),
                    'rsi_6': current_row.get('RSI_6'),
                    'rsi_12': current_row.get('RSI_12'),
                    'cci': current_row.get('CCI_20'),
                    'roc': current_row.get('ROC_12'),
                    'dmi_plus': current_row.get('DMP_14'),
                    'dmi_minus': current_row.get('DMN_14'),
                    'adx': current_row.get('ADX_14'),
                    'max_return': max_future_return,
                    'success': success
                })

        # 计算总体统计
        total_success = sum(stats['success'] for stats in signal_stats.values())
        total_signals = sum(stats['total'] for stats in signal_stats.values())
        overall_success_rate = round((total_success / total_signals * 100), 2) if total_signals > 0 else 0
        
        # 计算每种信号的成功率
        success_rates = {}
        for signal_type, stats in signal_stats.items():
            success_rate = round((stats['success'] / stats['total'] * 100), 2) if stats['total'] > 0 else 0
            success_rates[signal_type] = {
                'success_rate': success_rate,
                'total_signals': stats['total'],
                'success_count': stats['success']
            }

        # 最近信号检查部分
        recent_signals = []
        valuation_checked = 0
        valuation_blocked = 0
        valuation_missing = 0
        valuation_candidates = 0
        if len(df) >= 4:  # 改为4天以确保有足够数据计算3天的信号
            # 将索引转换为datetime类型
            df.index = pd.to_datetime(df.index)
            # 获取最后一个交易日
            last_date = df.index[-1]
            # 获取最近3个交易日的数据（排除周六和周日）
            trading_days = df[df.index.dayofweek < 5].index  # 0-4分别代表周一到周五
            last_3_trading_days = trading_days[-3:]
            last_3_days = df.loc[last_3_trading_days].copy()
            
            # 最后一个交易日是否为today
            if self.current_time != last_3_trading_days[-1].strftime('%Y-%m-%d'):
                return {
                    'signal_stats': 0,
                    'overall_success_rate': 0,
                    'total_signals': 0,
                    'total_success': 0,
                    'signals': [],
                    'recent_signals': []
                }
            
            for i in range(len(last_3_days)):
                current_row = last_3_days.iloc[i]
                if i > 0:
                    prev_row = last_3_days.iloc[i-1]
                else:
                    # 获取前一个交易日的数据
                    prev_date = trading_days[trading_days.get_loc(last_3_trading_days[0]) - 1]
                    prev_row = df.loc[prev_date]
                
                signals_for_day = []  # 存储当天的所有信号
                
                # KDJ信号判断
                if (current_row.get('K_9_3') is not None and current_row.get('D_9_3') is not None and
                    current_row['K_9_3'] < 20 and current_row['D_9_3'] < 20):
                    signals_for_day.append(('KDJ超卖', 'kdj_oversold'))
                if (prev_row.get('K_9_3') is not None and prev_row.get('D_9_3') is not None and
                    current_row.get('K_9_3') is not None and current_row.get('D_9_3') is not None and
                    prev_row['K_9_3'] < prev_row['D_9_3'] and 
                    current_row['K_9_3'] > current_row['D_9_3']):
                    signals_for_day.append(('KDJ金叉', 'kdj_golden_cross'))
                if (current_row.get('K_9_3') is not None and
                    current_row['close'] < df.iloc[-3:].iloc[:i+1]['close'].min() and 
                    current_row['K_9_3'] > df.iloc[-3:].iloc[:i+1]['K_9_3'].min()):
                    signals_for_day.append(('KDJ底背离', 'kdj_divergence'))
                    
                # MACD信号判断
                if (prev_row.get('MACD_12_26_9') is not None and prev_row.get('MACDs_12_26_9') is not None and
                    current_row.get('MACD_12_26_9') is not None and current_row.get('MACDs_12_26_9') is not None and
                    prev_row['MACD_12_26_9'] < prev_row['MACDs_12_26_9'] and 
                    current_row['MACD_12_26_9'] > current_row['MACDs_12_26_9']):
                    signals_for_day.append(('MACD金叉', 'macd_golden_cross'))
                if (prev_row.get('MACD_12_26_9') is not None and 
                    prev_row['MACD_12_26_9'] < 0 and 
                    current_row.get('MACD_12_26_9') is not None and
                    current_row['MACD_12_26_9'] > 0):
                    signals_for_day.append(('MACD零轴上穿', 'macd_zero_cross'))
                if (current_row.get('MACD_12_26_9') is not None and
                    current_row['close'] < df.iloc[-3:].iloc[:i+1]['close'].min() and 
                    current_row['MACD_12_26_9'] > df.iloc[-3:].iloc[:i+1]['MACD_12_26_9'].min()):
                    signals_for_day.append(('MACD底背离', 'macd_divergence'))
                
                # RSI信号判断
                if (current_row.get('RSI_6') is not None and current_row['RSI_6'] < 20):
                    signals_for_day.append(('RSI超卖', 'rsi_oversold'))
                if (prev_row.get('RSI_6') is not None and prev_row.get('RSI_12') is not None and
                    current_row.get('RSI_6') is not None and current_row.get('RSI_12') is not None and
                    prev_row['RSI_6'] < prev_row['RSI_12'] and 
                    current_row['RSI_6'] > current_row['RSI_12']):
                    signals_for_day.append(('RSI金叉', 'rsi_golden_cross'))
                
                # BOLL信号判断
                if (current_row.get('BBL_20_2.0') is not None and
                    current_row['close'] <= current_row['BBL_20_2.0'] * 1.01):
                    signals_for_day.append(('BOLL下轨支撑', 'boll_bottom_touch'))
                if (current_row.get('BBB_20_2.0') is not None and prev_row.get('BBB_20_2.0') is not None and
                    current_row['BBB_20_2.0'] > prev_row['BBB_20_2.0'] * 1.1):
                    signals_for_day.append(('BOLL带宽扩张', 'boll_width_expand'))
                
                # MA信号判断
                if (prev_row.get('SMA_5') is not None and prev_row.get('SMA_20') is not None and
                    current_row.get('SMA_5') is not None and current_row.get('SMA_20') is not None and
                    prev_row['SMA_5'] < prev_row['SMA_20'] and 
                    current_row['SMA_5'] > current_row['SMA_20']):
                    signals_for_day.append(('MA5上穿MA20', 'ma_golden_cross'))
                if (current_row.get('SMA_20') is not None and
                    current_row['close'] > current_row['SMA_20'] * 0.99 and 
                    current_row['close'] < current_row['SMA_20'] * 1.01):
                    signals_for_day.append(('MA20支撑', 'ma_support'))
                
                # DMI信号判断
                if (prev_row.get('DMP_14') is not None and prev_row.get('DMN_14') is not None and
                    current_row.get('DMP_14') is not None and current_row.get('DMN_14') is not None and
                    current_row.get('ADX_14') is not None and
                    prev_row['DMP_14'] < prev_row['DMN_14'] and 
                    current_row['DMP_14'] > current_row['DMN_14'] and 
                    current_row['ADX_14'] > 20):
                    signals_for_day.append(('DMI金叉', 'dmi_golden_cross'))
                if (current_row.get('ADX_14') is not None and current_row['ADX_14'] > 30):
                    signals_for_day.append(('ADX强势', 'dmi_adx_strong'))
                
                # CCI信号判断
                if (current_row.get('CCI_20') is not None and current_row['CCI_20'] < -100):
                    signals_for_day.append(('CCI超卖', 'cci_oversold'))
                if (prev_row.get('CCI_20') is not None and current_row.get('CCI_20') is not None and
                    prev_row['CCI_20'] < 0 and current_row['CCI_20'] > 0):
                    signals_for_day.append(('CCI零轴上穿', 'cci_zero_cross'))
                
                # ROC信号判断
                if (prev_row.get('ROC_12') is not None and current_row.get('ROC_12') is not None and
                    prev_row['ROC_12'] < 0 and current_row['ROC_12'] > 0):
                    signals_for_day.append(('ROC零轴上穿', 'roc_zero_cross'))
                if (current_row.get('ROC_12') is not None and
                    current_row['close'] < df.iloc[-3:].iloc[:i+1]['close'].min() and 
                    current_row['ROC_12'] > df.iloc[-3:].iloc[:i+1]['ROC_12'].min()):
                    signals_for_day.append(('ROC底背离', 'roc_divergence'))

                # 处理当天的所有信号
                for signal, signal_type in signals_for_day:
                    valuation_candidates += 1
                    current_pos = df.index.get_loc(last_3_days.index[i])
                    if self._is_st(df, current_pos):
                        continue
                    if not self._passes_trade_status(df, current_pos):
                        continue
                    if not self._passes_liquidity_filters(df, current_pos, signal_type):
                        continue
                    passed, status = self._check_valuation_filters(stock_code)
                    if status in ('passed', 'blocked'):
                        valuation_checked += 1
                    elif status == 'missing':
                        valuation_missing += 1
                    if status == 'blocked':
                        valuation_blocked += 1
                    if not passed:
                        continue
                    if (signal_stats[signal_type]['total'] > 8 and 
                        success_rates[signal_type]['success_rate'] >= 60 and  
                        overall_success_rate >= 50):
                        
                        # 根据信号类型收集对应的指标数据
                        signal_data = {
                            'date': last_3_days.index[i],
                            'signal_type': signal_type,
                            'signal': signal,
                            'close': current_row['close'],
                            'signal_total': signal_stats[signal_type]['total'],
                            'signal_success_rate': success_rates[signal_type]['success_rate'],
                            'overall_success_rate': overall_success_rate
                        }
                        
                        # 添加对应的技术指标数据
                        if signal_type.startswith('kdj'):
                            signal_data.update({
                                'k_value': current_row.get('K_9_3'),
                                'd_value': current_row.get('D_9_3'),
                                'j_value': current_row.get('J_9_3')
                            })
                        elif signal_type.startswith('macd'):
                            signal_data.update({
                                'macd': current_row.get('MACD_12_26_9'),
                                'macd_signal': current_row.get('MACDs_12_26_9')
                            })
                        elif signal_type.startswith('rsi'):
                            signal_data.update({
                                'RSI_6': current_row.get('RSI_6'),
                                'RSI_12': current_row.get('RSI_12')
                            })
                        elif signal_type.startswith('boll'):
                            signal_data.update({
                                'BBL_20_2.0': current_row.get('BBL_20_2.0'),
                                'BBM_20_2.0': current_row.get('BBM_20_2.0'),
                                'BBU_20_2.0': current_row.get('BBU_20_2.0')
                            })
                        elif signal_type.startswith('ma'):
                            signal_data.update({
                                'SMA_5': current_row.get('SMA_5'),
                                'SMA_20': current_row.get('SMA_20')
                            })
                        elif signal_type.startswith('dmi'):
                            signal_data.update({
                                'DMP_14': current_row.get('DMP_14'),
                                'DMN_14': current_row.get('DMN_14'),
                                'ADX_14': current_row.get('ADX_14')
                            })
                        elif signal_type.startswith('cci'):
                            signal_data.update({
                                'CCI_20': current_row.get('CCI_20')
                            })
                        elif signal_type.startswith('roc'):
                            signal_data.update({
                                'ROC_12': current_row.get('ROC_12')
                            })
                        
                        recent_signals.append(signal_data)

        if valuation_checked > 0:
            hit_rate = round(valuation_blocked / valuation_checked * 100, 2)
            self.logger.warning(
                f"股票 {stock_code} 估值过滤命中率: {hit_rate}% "
                f"(过滤 {valuation_blocked}/{valuation_checked}, 缺失 {valuation_missing}, 触发 {valuation_candidates})"
            )
        return {
            'signal_stats': success_rates,
            'overall_success_rate': overall_success_rate,
            'total_signals': total_signals,
            'total_success': total_success,
            'signals': signals,
            'recent_signals': recent_signals
        }
    
    def close(self, reason):
        """关闭数据库连接"""
        self.conn.close()
