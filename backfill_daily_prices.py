#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
补充现有数据库中信号的每日价格数据
为历史信号补充每日价格数据，用于准确计算止盈止损
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
import argparse
import json
import urllib.request
import urllib.parse
import time

# 添加项目路径
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'Spiders'))

from Spiders.spiders.stock_config import KLINE_API, KLINE_FIELD_MAPPING, STOCK_PREFIX_MAP, HEADERS

DB_PATH = os.path.join(os.path.dirname(__file__), 'stock_signals.db')

def ensure_table_exists():
    """确保 stock_signal_daily_prices 表存在"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 创建每日价格数据表
        cursor.execute('''
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
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_signal_daily_prices_signal_id ON stock_signal_daily_prices(signal_id)')
        except:
            pass
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_signal_daily_prices_stock_code ON stock_signal_daily_prices(stock_code)')
        except:
            pass
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_signal_daily_prices_date ON stock_signal_daily_prices(date)')
        except:
            pass
        
        conn.commit()
    finally:
        conn.close()

def get_signals_without_daily_prices(stock_codes=None, start_date=None, end_date=None):
    """获取所有缺少每日价格数据的信号"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 构建查询条件
    query = '''
        SELECT DISTINCT s.id, s.stock_code, s.stock_name, s.insert_date, s.insert_price
        FROM stock_signals s
        LEFT JOIN stock_signal_daily_prices p ON s.id = p.signal_id
        WHERE p.id IS NULL
    '''
    params = []
    
    if stock_codes:
        placeholders = ','.join(['?'] * len(stock_codes))
        query += f' AND s.stock_code IN ({placeholders})'
        params.extend(stock_codes)
    
    if start_date:
        query += ' AND s.insert_date >= ?'
        params.append(start_date)
    
    if end_date:
        query += ' AND s.insert_date <= ?'
        params.append(end_date)
    
    query += ' ORDER BY s.insert_date DESC'
    
    cursor.execute(query, params)
    signals = cursor.fetchall()
    conn.close()
    
    return signals

def backfill_daily_prices_for_signal(signal_id, stock_code, stock_name, insert_date, insert_price, df):
    """为单个信号补充每日价格数据"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 确保DataFrame的索引是datetime类型
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        # 找到最接近的交易日
        if isinstance(insert_date, str):
            insert_date = pd.to_datetime(insert_date)
        
        # 找到insert_date当天或之后的第一天（交易日）
        future_dates = df.index[df.index >= insert_date]
        if len(future_dates) == 0:
            return False, f"没有找到insert_date ({insert_date.strftime('%Y-%m-%d')}) 之后的数据"
        
        nearest_date = future_dates[0]
        created_idx = df.index.get_loc(nearest_date)
        
        # 获取从nearest_date当天开始，后续30天的数据（共31天：当天+后30天）
        # 限制最多30个交易日的数据
        end_idx = min(created_idx + 31, len(df))
        future_data = df.iloc[created_idx:end_idx]
        
        if future_data.empty:
            return False, "没有未来数据"
        
        # 确保close列中没有None值（过滤掉无效数据）
        future_data = future_data[future_data['close'].notna()]
        
        if future_data.empty:
            return False, "没有有效的收盘价数据"
        
        # 限制为最多30天的数据
        if len(future_data) > 30:
            future_data = future_data.iloc[:30]
        
        # 保存每日价格数据
        saved_count = 0
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for idx, (date, row) in enumerate(future_data.iterrows()):
            days_from_signal = idx  # 从0开始，0表示信号当天
            
            # 删除旧数据（如果存在）
            cursor.execute('''
                DELETE FROM stock_signal_daily_prices
                WHERE signal_id=? AND date=?
            ''', (signal_id, date.strftime("%Y-%m-%d")))
            
            # 插入新数据
            cursor.execute('''
                INSERT INTO stock_signal_daily_prices (
                    signal_id, stock_code, date, open, high, low, close,
                    days_from_signal, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                signal_id,
                stock_code,
                date.strftime("%Y-%m-%d"),
                round(row.get('open', 0), 2) if pd.notna(row.get('open')) else None,
                round(row.get('high', 0), 2) if pd.notna(row.get('high')) else None,
                round(row.get('low', 0), 2) if pd.notna(row.get('low')) else None,
                round(row.get('close', 0), 2) if pd.notna(row.get('close')) else None,
                days_from_signal,
                current_time
            ))
            saved_count += 1
        
        conn.commit()
        return True, f"保存了 {saved_count} 天的价格数据（信号日期后30天内）"
        
    except Exception as e:
        conn.rollback()
        return False, f"处理失败: {str(e)}"
    finally:
        conn.close()

def fetch_kline_data_for_backfill(stock_code, start_date, end_date, verbose=False):
    """
    获取股票的K线数据（用于数据补充）
    使用爬虫的API逻辑获取数据
    """
    try:
        # 获取股票代码前缀对应的数字
        prefix = STOCK_PREFIX_MAP.get(stock_code[:2])
        if not prefix:
            if verbose:
                print(f"    错误: 不支持的股票代码前缀: {stock_code[:2]}")
            return None
        
        # 构建API请求参数
        params = {
            'secid': f"{prefix}.{stock_code[2:]}",
            'fields1': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
            'klt': KLINE_API['klt']['daily'],
            'fqt': KLINE_API['fqt']['forward'],
            'ut': KLINE_API['ut'],
            'beg': start_date or '',
            'end': end_date or '',
            'lmt': '1000'  # 限制返回1000条数据
        }
        
        url = f"{KLINE_API['base_url']}?" + "&".join([f"{k}={v}" for k, v in params.items()])
        
        if verbose:
            print(f"    请求URL: {url}")
        
        # 发送请求
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=30) as response:
            response_text = response.read().decode('utf-8')
            data = json.loads(response_text)
            
            if verbose:
                print(f"    API响应: {json.dumps(data, ensure_ascii=False)[:200]}...")
            
            if data.get('data'):
                klines = data['data'].get('klines', [])
                
                if not klines:
                    if verbose:
                        print(f"    警告: klines为空数组，可能是日期范围内没有数据")
                        print(f"    尝试不指定日期范围...")
                    # 如果指定日期范围没有数据，尝试不指定日期范围
                    if start_date or end_date:
                        return fetch_kline_data_for_backfill(stock_code, None, None, verbose=False)
                    return None
                
                if verbose:
                    print(f"    获取到 {len(klines)} 条K线数据")
                
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
                if not df.empty and 'date' in df.columns:
                    df.set_index('date', inplace=True)
                    df.index = pd.to_datetime(df.index)
                    if verbose:
                        print(f"    DataFrame创建成功，共 {len(df)} 行")
                    return df
                else:
                    if verbose:
                        print(f"    错误: DataFrame为空或缺少date列")
            else:
                if verbose:
                    error_msg = data.get('message', '未知错误')
                    print(f"    错误: API返回数据格式不正确: {error_msg}")
                    if 'data' in data:
                        print(f"    data字段: {data.get('data')}")
        
        return None
    except urllib.error.HTTPError as e:
        if verbose:
            print(f"    HTTP错误: {e.code} - {e.reason}")
            try:
                error_body = e.read().decode('utf-8')
                print(f"    错误详情: {error_body[:200]}")
            except:
                pass
        return None
    except urllib.error.URLError as e:
        if verbose:
            print(f"    网络错误: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        if verbose:
            print(f"    JSON解析错误: {str(e)}")
        return None
    except Exception as e:
        if verbose:
            import traceback
            print(f"    获取K线数据失败: {str(e)}")
            print(f"    错误堆栈: {traceback.format_exc()}")
        return None

def main():
    parser = argparse.ArgumentParser(description='补充现有数据库中信号的每日价格数据')
    parser.add_argument('--stock-codes', nargs='+', help='指定股票代码列表（可选）')
    parser.add_argument('--start-date', help='开始日期（格式：YYYY-MM-DD）')
    parser.add_argument('--end-date', help='结束日期（格式：YYYY-MM-DD）')
    parser.add_argument('--dry-run', action='store_true', help='仅显示需要补充的信号，不实际补充')
    parser.add_argument('--delay', type=float, default=0.5, help='请求之间的延迟（秒），默认0.5秒')
    parser.add_argument('--batch-size', type=int, default=10, help='每批处理的股票数量，默认10')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("开始补充现有数据库中的每日价格数据")
    print("=" * 80)
    print(f"延迟设置: {args.delay}秒/请求")
    print(f"批次大小: {args.batch_size}只股票/批")
    print("=" * 80)
    
    # 确保表存在
    print("\n检查数据库表...")
    ensure_table_exists()
    print("✓ 数据库表检查完成")
    
    # 获取所有缺少每日价格数据的信号
    signals = get_signals_without_daily_prices(
        stock_codes=args.stock_codes,
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    if not signals:
        print("所有信号都已包含每日价格数据，无需补充")
        return
    
    print(f"\n找到 {len(signals)} 个需要补充的信号")
    
    if args.dry_run:
        print("\n需要补充的信号列表（dry-run模式，不会实际补充）：")
        for signal in signals[:10]:  # 只显示前10个
            signal_id, stock_code, stock_name, insert_date, insert_price = signal
            print(f"  - ID {signal_id}: {stock_code} ({stock_name}), 日期: {insert_date}")
        if len(signals) > 10:
            print(f"  ... 还有 {len(signals) - 10} 个信号")
        return
    
    # 按股票代码分组，批量处理
    signals_by_stock = {}
    for signal in signals:
        signal_id, stock_code, stock_name, insert_date, insert_price = signal
        if stock_code not in signals_by_stock:
            signals_by_stock[stock_code] = []
        signals_by_stock[stock_code].append({
            'id': signal_id,
            'stock_name': stock_name,
            'insert_date': insert_date,
            'insert_price': insert_price
        })
    
    print(f"涉及 {len(signals_by_stock)} 只股票\n")
    
    # 处理每只股票
    total_processed = 0
    total_failed = 0
    stock_list = list(signals_by_stock.items())
    total_stocks = len(stock_list)
    
    # 分批处理
    for batch_start in range(0, total_stocks, args.batch_size):
        batch_end = min(batch_start + args.batch_size, total_stocks)
        batch = stock_list[batch_start:batch_end]
        
        print(f"\n处理批次 {batch_start // args.batch_size + 1} (股票 {batch_start + 1}-{batch_end}/{total_stocks})")
        print("-" * 80)
        
        for idx, (stock_code, stock_signals) in enumerate(batch, batch_start + 1):
            print(f"\n[{idx}/{total_stocks}] 处理股票: {stock_code} ({stock_signals[0]['stock_name']})")
            print(f"  需要处理 {len(stock_signals)} 个信号")
            
            # 获取该股票的所有信号日期
            dates = [s['insert_date'] for s in stock_signals]
            min_date = min(dates)
            max_date = max(dates)
            
            # 计算需要获取的日期范围
            # 策略：获取从最早信号当天到最新信号后30天的数据，但不超过今天
            # 每只股票只获取一次K线数据，然后为所有信号提取各自需要的30天数据
            today = datetime.now().date()
            min_date_dt = pd.to_datetime(min_date).date()
            max_date_dt = pd.to_datetime(max_date).date()
            
            # 开始日期：最早信号当天（不需要历史数据，只需要从信号日期开始）
            start_date_dt = min_date_dt
            # 结束日期：最新信号+30天，但不超过今天
            end_date_dt = min(max_date_dt + timedelta(days=30), today)
            
            start_date = start_date_dt.strftime("%Y-%m-%d")
            end_date = end_date_dt.strftime("%Y-%m-%d")
            
            print(f"  需要K线数据范围: {start_date} 到 {end_date} (今天: {today.strftime('%Y-%m-%d')}, 范围: {(end_date_dt - start_date_dt).days}天)")
            print(f"  说明: 每只股票只获取一次K线数据，然后为每个信号提取其后30天的价格数据")
            
            # 获取K线数据（第一个股票使用verbose模式，便于调试）
            verbose = (idx == 1)
            df = fetch_kline_data_for_backfill(stock_code, start_date, end_date, verbose=verbose)
            
            # 如果指定日期范围没有数据，尝试不指定日期范围（获取所有可用数据）
            if (df is None or df.empty) and verbose:
                print(f"  尝试不指定日期范围获取数据...")
                df = fetch_kline_data_for_backfill(stock_code, None, None, verbose=True)
            
            if df is None or df.empty:
                print(f"  ✗ 无法获取 {stock_code} 的K线数据，跳过")
                # 如果第一个失败，再试一次并显示详细信息
                if idx == 1:
                    print(f"  重试获取 {stock_code} 的K线数据（详细模式）...")
                    df = fetch_kline_data_for_backfill(stock_code, start_date, end_date, verbose=True)
                    if df is None or df.empty:
                        print(f"  ✗ 重试仍然失败")
                total_failed += len(stock_signals)
                # 添加延迟，避免请求过快
                time.sleep(args.delay)
                continue
            
            print(f"  ✓ 获取到 {len(df)} 天的K线数据（所有信号共享此数据）")
            
            # 处理每个信号：从共享的K线数据中提取该信号日期后30天的数据
            signal_success = 0
            signal_failed = 0
            for signal in stock_signals:
                success, message = backfill_daily_prices_for_signal(
                    signal['id'],
                    stock_code,
                    signal['stock_name'],
                    signal['insert_date'],
                    signal['insert_price'],
                    df
                )
                
                if success:
                    print(f"    ✓ 信号ID {signal['id']} ({signal['insert_date']}): {message}")
                    signal_success += 1
                    total_processed += 1
                else:
                    print(f"    ✗ 信号ID {signal['id']} ({signal['insert_date']}): {message}")
                    signal_failed += 1
                    total_failed += 1
            
            print(f"  股票 {stock_code} 处理完成: 成功 {signal_success}, 失败 {signal_failed}")
            
            # 添加延迟，避免请求过快
            time.sleep(args.delay)
        
        # 批次之间的延迟稍长
        if batch_end < total_stocks:
            print(f"\n批次完成，等待 {args.delay * 2} 秒后继续下一批...")
            time.sleep(args.delay * 2)
    
    print("\n" + "=" * 80)
    print(f"补充完成！")
    print(f"  成功: {total_processed} 个信号")
    print(f"  失败: {total_failed} 个信号")
    print(f"  成功率: {total_processed / (total_processed + total_failed) * 100:.2f}%")
    print("=" * 80)

if __name__ == "__main__":
    main()
