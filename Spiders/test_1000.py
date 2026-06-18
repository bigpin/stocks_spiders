"""测试 1000 只股票的 K 线拉取 + 信号分析（含重试逻辑验证）"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spiders.stock_kline import StockKlineSpider
from spiders.baostock_helper import read_stock_list_txt


def main():
    stock_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'stock_list.txt')
    codes, _ = read_stock_list_txt(stock_file)
    test_codes = codes[150:500]
    print(f"总股票数: {len(codes)}，本次测试: {len(test_codes)} 只")

    spider = StockKlineSpider(
        stock_codes=','.join(test_codes),
        calc_indicators=True,
    )
    try:
        spider.run()
    finally:
        spider.cleanup()


if __name__ == '__main__':
    main()
