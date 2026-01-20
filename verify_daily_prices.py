#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
验证每日价格数据的补充结果
检查哪些信号有每日价格数据，哪些没有
"""

import sqlite3
import os
import argparse
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), 'stock_signals.db')

def verify_daily_prices(stock_codes=None, start_date=None, end_date=None):
    """验证每日价格数据的完整性"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 构建查询条件
    query = '''
        SELECT s.id, s.stock_code, s.stock_name, s.insert_date, 
               COUNT(p.id) as price_days
        FROM stock_signals s
        LEFT JOIN stock_signal_daily_prices p ON s.id = p.signal_id
        WHERE 1=1
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
    
    query += ' GROUP BY s.id ORDER BY s.insert_date DESC'
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    conn.close()
    
    # 统计结果
    stats = {
        'total': len(results),
        'with_prices': 0,
        'without_prices': 0,
        'incomplete': 0,  # 有数据但不足30天
        'complete': 0,    # 有30天或更多数据
        'by_stock': defaultdict(lambda: {'total': 0, 'with_prices': 0, 'without_prices': 0})
    }
    
    signals_without_prices = []
    signals_incomplete = []
    
    for signal_id, stock_code, stock_name, insert_date, price_days in results:
        stats['by_stock'][stock_code]['total'] += 1
        
        if price_days == 0:
            stats['without_prices'] += 1
            stats['by_stock'][stock_code]['without_prices'] += 1
            signals_without_prices.append({
                'id': signal_id,
                'stock_code': stock_code,
                'stock_name': stock_name,
                'insert_date': insert_date
            })
        else:
            stats['with_prices'] += 1
            stats['by_stock'][stock_code]['with_prices'] += 1
            
            if price_days < 30:
                stats['incomplete'] += 1
                signals_incomplete.append({
                    'id': signal_id,
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'insert_date': insert_date,
                    'price_days': price_days
                })
            else:
                stats['complete'] += 1
    
    return stats, signals_without_prices, signals_incomplete

def main():
    parser = argparse.ArgumentParser(description='验证每日价格数据的补充结果')
    parser.add_argument('--stock-codes', nargs='+', help='指定股票代码列表（可选）')
    parser.add_argument('--start-date', help='开始日期（格式：YYYY-MM-DD）')
    parser.add_argument('--end-date', help='结束日期（格式：YYYY-MM-DD）')
    parser.add_argument('--show-details', action='store_true', help='显示详细信息')
    parser.add_argument('--show-missing', action='store_true', help='显示缺少价格数据的信号列表')
    parser.add_argument('--show-incomplete', action='store_true', help='显示数据不完整的信号列表')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("验证每日价格数据的补充结果")
    print("=" * 80)
    
    stats, signals_without_prices, signals_incomplete = verify_daily_prices(
        stock_codes=args.stock_codes,
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    print(f"\n总体统计:")
    print(f"  总信号数: {stats['total']}")
    print(f"  有价格数据: {stats['with_prices']} ({stats['with_prices'] / stats['total'] * 100:.2f}%)")
    print(f"  无价格数据: {stats['without_prices']} ({stats['without_prices'] / stats['total'] * 100:.2f}%)")
    print(f"  完整数据(≥30天): {stats['complete']} ({stats['complete'] / stats['total'] * 100:.2f}%)")
    print(f"  不完整数据(<30天): {stats['incomplete']} ({stats['incomplete'] / stats['total'] * 100:.2f}%)")
    
    if args.show_details:
        print(f"\n按股票统计:")
        for stock_code, stock_stats in sorted(stats['by_stock'].items()):
            total = stock_stats['total']
            with_prices = stock_stats['with_prices']
            without_prices = stock_stats['without_prices']
            print(f"  {stock_code}:")
            print(f"    总数: {total}, 有数据: {with_prices}, 无数据: {without_prices}")
    
    if args.show_missing and signals_without_prices:
        print(f"\n缺少价格数据的信号 ({len(signals_without_prices)} 个):")
        for signal in signals_without_prices[:20]:  # 只显示前20个
            print(f"  - ID {signal['id']}: {signal['stock_code']} ({signal['stock_name']}), 日期: {signal['insert_date']}")
        if len(signals_without_prices) > 20:
            print(f"  ... 还有 {len(signals_without_prices) - 20} 个信号")
    
    if args.show_incomplete and signals_incomplete:
        print(f"\n数据不完整的信号 ({len(signals_incomplete)} 个):")
        for signal in signals_incomplete[:20]:  # 只显示前20个
            print(f"  - ID {signal['id']}: {signal['stock_code']} ({signal['stock_name']}), "
                  f"日期: {signal['insert_date']}, 天数: {signal['price_days']}")
        if len(signals_incomplete) > 20:
            print(f"  ... 还有 {len(signals_incomplete) - 20} 个信号")
    
    print("\n" + "=" * 80)
    
    # 返回退出码
    if stats['without_prices'] > 0:
        print("警告: 仍有信号缺少价格数据，建议运行 backfill_daily_prices.py 补充")
        return 1
    else:
        print("✓ 所有信号都有价格数据")
        return 0

if __name__ == "__main__":
    exit(main())
