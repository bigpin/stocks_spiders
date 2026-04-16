import os

# 东方财富API配置
EASTMONEY_API = {
    'base_url': 'https://push2.eastmoney.com/api/qt/stock/get',
    'fields': 'f43,f44,f45,f46,f47,f48,f49,f50,f51,f52,f55,f57,f58,f60,f71,f92,f105,f116,f117,f162,f167,f168,f169,f170,f171,f177,f183,f184,f185,f186,f187,f188,f189,f190,f191,f192,f193,f194,f195,f196,f197',
    'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
    'wbp2u': '|0|0|0|web'
}

# 字段映射关系
FIELD_MAPPING = {
    'f58': 'stock_name',          # 股票名称
    'f43': 'new_price',           # 最新价
    'f169': 'price_change',       # 涨跌额
    'f170': 'percentage_change',   # 涨跌幅
    'f46': 'opening_price',       # 今开
    'f44': 'highest_price',       # 最高
    'f45': 'lowest_price',        # 最低
    'f60': 'closing_price',       # 昨收
    'f47': 'trading_volume',      # 成交量
    'f48': 'trading_value',       # 成交额
    'f168': 'turnover_rate',      # 换手率
    'f162': 'pe',                 # 市盈率
    'f167': 'pb'                  # 市净率
}

# 数据处理规则（除数）
FIELD_DIVISORS = {
    'new_price': 100,
    'price_change': 100,
    'percentage_change': 100,
    'opening_price': 100,
    'highest_price': 100,
    'lowest_price': 100,
    'closing_price': 100,
    'trading_volume': 100,
    'trading_value': 10000,  # 转换为亿
    'turnover_rate': 100,
    'pe': 100,
    'pb': 100
}

# 请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://quote.eastmoney.com/',
    'Accept': '*/*'
}

# 股票代码前缀映射（用于东方财富API）
STOCK_PREFIX_MAP = {
    'sh': '1',  # 上海证券交易所
    'sz': '0',  # 深圳证券交易所
    '92': '0'   # 北京证券交易所（北交所）
}

# 股票代码前缀映射（用于baostock）
BAOSTOCK_PREFIX_MAP = {
    'sh': 'sh',  # 上海证券交易所
    'sz': 'sz',  # 深圳证券交易所
    '92': 'bj'   # 北京证券交易所（北交所）
}

# 数据源配置：'eastmoney' 或 'baostock'
DATA_SOURCE = 'baostock'  # 默认使用 baostock

# baostock 并行拉取进程数（仅当 DATA_SOURCE='baostock' 时生效）
# 实测 8 进程稳定；12 进程会出现大量 Broken pipe，不建议超过 8
BAOSTOCK_FETCH_WORKERS = 5

# baostock 并行模式是否在子进程内“拉取后立即计算信号”（流水线模式）
# True：每个子进程 fetch K线 -> 计算指标/信号 -> 返回结果给主进程做 I/O（写文件/SQLite/导出）
# False：维持旧模式（先拉完全部，再单独开计算进程池）
BAOSTOCK_PIPELINE_FETCH_AND_COMPUTE = True

# K 线拉取完成后，信号计算（指标+analyze_signals）的并行进程数；0 表示串行
# 纯 CPU 计算，与 baostock 无关；设为 CPU 核数即可（过多会增加内存和 pickle 开销）
PROCESS_KLINE_WORKERS = min(8, os.cpu_count() or 4)


# K线数据
# stock_config.py 添加K线相关配置

# K线数据API配置
KLINE_API = {
    'base_url': 'https://push2his.eastmoney.com/api/qt/stock/kline/get',
    'fields': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
    'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
    'klt': {
        '1min': '1',    # 1分钟
        '5min': '5',    # 5分钟
        '15min': '15',  # 15分钟
        '30min': '30',  # 30分钟
        '60min': '60',  # 60分钟
        'daily': '101', # 日线
        'weekly': '102',# 周线
        'monthly': '103'# 月线
    },
    'fqt': {
        'none': '0',    # 不复权
        'forward': '1', # 前复权
        'backward': '2' # 后复权
    }
}

# K线数据字段映射
KLINE_FIELD_MAPPING = {
    0: 'date',          # 日期
    1: 'open',          # 开盘价
    2: 'close',         # 收盘价
    3: 'high',          # 最高价
    4: 'low',           # 最低价
    5: 'volume',        # 成交量
    6: 'amount',        # 成交额
    7: 'amplitude',     # 振幅
    8: 'change_rate',   # 涨跌幅
    9: 'change_amount', # 涨跌额
    10: 'turnover'      # 换手率
}

# 技术指标
# 技术指标配置
INDICATORS_CONFIG = {
    'kdj': {
        'period': 9,    # 默认周期
        'signal': 3,    # 信号周期
    },
    'macd': {
        'fast': 12,     # 快线周期
        'slow': 26,     # 慢线周期
        'signal': 9,    # 信号周期
    },
    'rsi': {
        'periods': [6, 12, 24]  # RSI周期
    },
    'boll': {
        'period': 20,   # 布林带周期
        'std': 2        # 标准差倍数
    },
    'ma': {
        'periods': [5, 10, 20, 30, 60]  # 移动平均线周期
    },
    'ema': {
        'periods': [5, 10, 20, 30, 60]  # 指数移动平均线周期
    },
    'wma': {
        'periods': [5, 10, 20, 30, 60]  # 加权移动平均线周期
    },
    'vwap': {},  # 成交量加权平均价格，不需要参数
    'atr': {
        'period': 14    # ATR周期
    },
    'dmi': {
        'length': 14,   # DMI周期
        'signal': 14    # ADX平滑周期
    },
    'cci': {
        'length': 20    # CCI周期
    },
    'obv': {},  # 能量潮指标，不需要参数
    'roc': {
        'length': 12    # ROC周期
    }
}

# 信号过滤与质量控制配置
SIGNAL_FILTERS = {
    # 指标与统计所需最少历史K线天数（保障MA60/成交量均值等稳定）
    'min_history_days': 60,
    # 成功率统计窗口
    'success_window_days': 14,
    # 流动性与量价过滤
    'liquidity': {
        'avg_days': 20,               # 计算均值的窗口
        'min_avg_amount': 1e8,        # 近20日平均成交额下限（单位：元）
        'min_avg_turnover_rate': 1.0, # 近20日平均换手率下限（%）
        'min_volume_ratio': 0.7,      # 当日成交量 / 20日均量 最低比例
        'trend_volume_ratio': 1.2     # 趋势类信号要求放量比例
    },
    # 估值过滤（如无估值数据则跳过）
    'valuation': {
        'enable': True,
        'pe_min': 0,
        'pe_max': 80,
        'pb_min': 0,
        'pb_max': 8
    },
    # 其他过滤
    'exclude_st': True,
    'require_tradestatus': True,
    # 量能热度分（0–100，仅展示/排序，不增加过滤门槛）
    'volume_heat': {
        'enable': True,
        'ma_short': 5,           # 量趋势：MA(短)/MA(长)
        'ma_long': 20,
        'ma_vol_recent': 3,      # 「放量」项：近 N 日均量 / MA20，降噪单日拉爆
        'percentile_lookback': 120,
        'percentile_min_samples': 30,  # 低于此样本数则量趋势退回固定区间线性映射
        'use_percentile_trend': True,  # True：量趋势用历史分位数映射到满分
        # 分项满分（量趋势 + 近N日放量 + 成交额）
        'weights': {'trend': 40, 'vol_recent': 25, 'amount': 35},
        # 流动性折扣：总分 × min(1, 近20日均成交额 / liquidity_floor)
        'liquidity_floor': 1e8,
    }
}
