import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator, ROCIndicator, StochasticOscillator
from ta.trend import MACD, EMAIndicator, SMAIndicator, ADXIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator, VolumeWeightedAveragePrice

class TechnicalIndicators:
    @staticmethod
    def calculate_kdj(df, period=9, signal=3):
        if len(df) < period + signal:
            return df
        try:
            stoch = StochasticOscillator(high=df['high'], low=df['low'], close=df['close'], window=period, smooth_window=signal)
            df[f'K_{period}_{signal}'] = stoch.stoch()
            df[f'D_{period}_{signal}'] = stoch.stoch_signal()
            df[f'J_{period}_{signal}'] = 3 * df[f'K_{period}_{signal}'] - 2 * df[f'D_{period}_{signal}']
        except Exception as e:
            print(f"计算KDJ时出错: {str(e)}")
        return df
    
    @classmethod
    def calculate_macd(cls, df, fast=12, slow=26, signal=9):
        if len(df) < slow + signal:
            return df
        try:
            macd = MACD(close=df['close'], window_fast=fast, window_slow=slow, window_sign=signal)
            df[f'MACD_{fast}_{slow}_{signal}'] = macd.macd()
            df[f'MACDs_{fast}_{slow}_{signal}'] = macd.macd_signal()
            df[f'MACDh_{fast}_{slow}_{signal}'] = macd.macd_diff()
        except Exception as e:
            print(f"计算MACD时出错: {str(e)}")
        return df
    
    @staticmethod
    def calculate_rsi(df, periods=[6, 12, 24]):
        max_period = max(periods) if periods else 24
        if len(df) < max_period:
            return df
        try:
            for period in periods:
                if len(df) >= period:
                    rsi = RSIIndicator(close=df['close'], window=period)
                    df[f'RSI_{period}'] = rsi.rsi()
        except Exception as e:
            print(f"计算RSI时出错: {str(e)}")
        return df
    
    @staticmethod
    def calculate_boll(df, period=20, std=2):
        if len(df) < period:
            return df
        try:
            bollinger = BollingerBands(close=df['close'], window=period, window_dev=std)
            df[f'BBL_{period}_{std}.0'] = bollinger.bollinger_lband()
            df[f'BBU_{period}_{std}.0'] = bollinger.bollinger_hband()
            df[f'BBM_{period}_{std}.0'] = bollinger.bollinger_mavg()
            df[f'BBB_{period}_{std}.0'] = df[f'BBU_{period}_{std}.0'] - df[f'BBL_{period}_{std}.0']
            df[f'BBP_{period}_{std}.0'] = (df['close'] - df[f'BBL_{period}_{std}.0']) / df[f'BBB_{period}_{std}.0']
        except Exception as e:
            print(f"计算Bollinger Bands时出错: {str(e)}")
        return df
    
    @staticmethod
    def calculate_ma(df, periods=[5, 10, 20, 30, 60]):
        try:
            for period in periods:
                if len(df) >= period:
                    sma = SMAIndicator(close=df['close'], window=period)
                    df[f'SMA_{period}'] = sma.sma_indicator()
        except Exception as e:
            print(f"计算MA时出错: {str(e)}")
        return df
    
    @staticmethod
    def calculate_ema(df, periods=[5, 10, 20, 30, 60]):
        try:
            for period in periods:
                if len(df) >= period:
                    ema = EMAIndicator(close=df['close'], window=period)
                    df[f'EMA_{period}'] = ema.ema_indicator()
        except Exception as e:
            print(f"计算EMA时出错: {str(e)}")
        return df
    
    @staticmethod
    def calculate_wma(df, periods=[5, 10, 20, 30, 60]):
        try:
            for period in periods:
                if len(df) >= period:
                    weights = np.arange(1, period + 1)
                    df[f'WMA_{period}'] = df['close'].rolling(window=period).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)
        except Exception as e:
            print(f"计算WMA时出错: {str(e)}")
        return df
    
    @staticmethod
    def calculate_vwap(df):
        try:
            df = df.sort_index()
            vwap = VolumeWeightedAveragePrice(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'])
            df['VWAP'] = vwap.volume_weighted_average_price()
            return df
        except Exception as e:
            print(f"计算VWAP时出错: {str(e)}")
            return df
    
    @staticmethod
    def calculate_atr(df, period=14):
        if len(df) < period:
            return df
        try:
            atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=period)
            df[f'ATRr_{period}'] = atr.average_true_range()
        except Exception as e:
            print(f"计算ATR时出错: {str(e)}")
        return df
    
    @staticmethod
    def calculate_dmi(df, length=14, signal=14):
        if len(df) < length * 2:
            return df
        try:
            adx = ADXIndicator(high=df['high'], low=df['low'], close=df['close'], window=length)
            df[f'ADX_{length}'] = adx.adx()
            df[f'ADXr_{length}'] = adx.adx()
            df[f'DMP_{length}'] = adx.adx_pos()
            df[f'DMN_{length}'] = adx.adx_neg()
        except Exception as e:
            print(f"计算DMI时出错: {str(e)}")
        return df
    
    @staticmethod
    def calculate_cci(df, length=20):
        if len(df) < length:
            return df
        try:
            df = df.sort_index()
            
            tp = (df['high'] + df['low'] + df['close']) / 3
            
            ma = tp.rolling(window=length).mean()
            
            md = tp.rolling(window=length).apply(lambda x: abs(x - x.mean()).mean())
            
            df['CCI_20'] = (tp - ma) / (0.015 * md)
            
            return df
        except Exception as e:
            print(f"计算CCI时出错: {str(e)}")
            return df
    
    @staticmethod
    def calculate_obv(df):
        if len(df) < 1:
            return df
        try:
            obv = OnBalanceVolumeIndicator(close=df['close'], volume=df['volume'])
            df['OBV'] = obv.on_balance_volume()
        except Exception as e:
            print(f"计算OBV时出错: {str(e)}")
        return df
    
    @staticmethod
    def calculate_roc(df, length=12):
        if len(df) < length + 1:
            return df
        try:
            roc = ROCIndicator(close=df['close'], window=length)
            df[f'ROC_{length}'] = roc.roc()
        except Exception as e:
            print(f"计算ROC时出错: {str(e)}")
        return df
    
    @classmethod
    def calculate_all(cls, df, config):
        df = df.sort_index()
        
        df = cls.calculate_kdj(df, 
                              period=config['kdj']['period'],
                              signal=config['kdj']['signal'])
        
        df = cls.calculate_macd(df,
                               fast=config['macd']['fast'],
                               slow=config['macd']['slow'],
                               signal=config['macd']['signal'])
        
        df = cls.calculate_rsi(df, periods=config['rsi']['periods'])
        
        df = cls.calculate_boll(df,
                               period=config['boll']['period'],
                               std=config['boll']['std'])
        
        if 'ma' in config:
            df = cls.calculate_ma(df, periods=config['ma']['periods'])
        if 'ema' in config:
            df = cls.calculate_ema(df, periods=config['ema']['periods'])
        if 'wma' in config:
            df = cls.calculate_wma(df, periods=config['wma']['periods'])
        if 'vwap' in config:
            df = cls.calculate_vwap(df)
        if 'atr' in config:
            df = cls.calculate_atr(df, period=config['atr']['period'])
        if 'dmi' in config:
            df = cls.calculate_dmi(df, length=config['dmi']['length'], signal=config['dmi']['signal'])
        if 'cci' in config:
            df = cls.calculate_cci(df, length=config['cci']['length'])
        if 'obv' in config:
            df = cls.calculate_obv(df)
        if 'roc' in config:
            df = cls.calculate_roc(df, length=config['roc']['length'])
        
        return df
