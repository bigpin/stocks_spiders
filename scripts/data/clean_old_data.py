#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
清理数据库中去年的数据
删除2024年的所有记录
"""

import sqlite3
import os
from datetime import datetime

# 数据库路径（脚本在 scripts/data 目录下，需要向上两级到项目根）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, 'stock_signals.db')
WEB_DB_PATH = os.path.join(PROJECT_ROOT, 'Spiders', 'web', 'stock_signals.db')

def clean_old_data(db_path, year=2024):
    """
    清理指定年份的数据
    
    Args:
        db_path: 数据库文件路径
        year: 要删除的年份，默认为2024
    """
    if not os.path.exists(db_path):
        print(f"数据库文件不存在: {db_path}")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 统计删除前的记录数
        cursor.execute("SELECT COUNT(*) FROM stock_signals WHERE insert_date LIKE ?", (f"{year}-%",))
        signals_count_before = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM stock_data WHERE date LIKE ?", (f"{year}-%",))
        data_count_before = cursor.fetchone()[0]
        
        print(f"\n数据库: {db_path}")
        print(f"删除前统计:")
        print(f"  stock_signals 表中 {year} 年的记录数: {signals_count_before}")
        print(f"  stock_data 表中 {year} 年的记录数: {data_count_before}")
        
        if signals_count_before == 0 and data_count_before == 0:
            print(f"  没有找到 {year} 年的数据，无需删除")
            conn.close()
            return
        
        # 删除 stock_signals 表中2024年的数据
        cursor.execute("DELETE FROM stock_signals WHERE insert_date LIKE ?", (f"{year}-%",))
        signals_deleted = cursor.rowcount
        
        # 删除 stock_data 表中2024年的数据
        cursor.execute("DELETE FROM stock_data WHERE date LIKE ?", (f"{year}-%",))
        data_deleted = cursor.rowcount
        
        # 提交事务
        conn.commit()
        
        print(f"\n删除结果:")
        print(f"  已删除 stock_signals 记录: {signals_deleted} 条")
        print(f"  已删除 stock_data 记录: {data_deleted} 条")
        
        # 执行 VACUUM 来回收空间
        print(f"\n正在优化数据库...")
        cursor.execute("VACUUM")
        conn.commit()
        print(f"  数据库优化完成")
        
        conn.close()
        print(f"✓ 清理完成\n")
        
    except Exception as e:
        print(f"错误: 清理数据库时出错: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()

def main():
    """主函数"""
    import sys
    
    # 如果命令行参数指定了年份，使用指定的年份，否则使用去年
    if len(sys.argv) > 1:
        try:
            target_year = int(sys.argv[1])
        except ValueError:
            print(f"错误: 无效的年份参数: {sys.argv[1]}")
            print("用法: python clean_old_data.py [年份]")
            print("示例: python clean_old_data.py 2024")
            return
    else:
        current_year = datetime.now().year
        target_year = current_year - 1  # 默认删除去年的数据
    
    print("=" * 60)
    print(f"清理数据库中 {target_year} 年的数据")
    print("=" * 60)
    
    # 先显示将要删除的数据统计
    print("\n⚠️  警告: 此操作将永久删除数据，无法恢复！")
    print(f"即将删除 {target_year} 年的所有数据\n")
    
    # 清理主数据库
    if os.path.exists(DB_PATH):
        clean_old_data(DB_PATH, target_year)
    else:
        print(f"主数据库文件不存在: {DB_PATH}")
    
    # 清理Web数据库
    if os.path.exists(WEB_DB_PATH):
        clean_old_data(WEB_DB_PATH, target_year)
    else:
        print(f"Web数据库文件不存在: {WEB_DB_PATH}")
    
    print("=" * 60)
    print("所有数据库清理完成！")
    print("=" * 60)

if __name__ == "__main__":
    main()
