import scrapy
import json
from items import EastMoneyItem
from .baostock_helper import read_stock_list_txt
from .stock_config import (
    EASTMONEY_API,
    FIELD_MAPPING,
    FIELD_DIVISORS,
    HEADERS,
    STOCK_PREFIX_MAP
)

class StockDetailSpider(scrapy.Spider):
    name = "stock_detail"
    allowed_domains = ["eastmoney.com", "push2.eastmoney.com"]
    
    def __init__(self, stock_codes=None, use_file=False, stock_file='stock_list.txt', *args, **kwargs):
        super(StockDetailSpider, self).__init__(*args, **kwargs)
        self._seen = 0
        if use_file and use_file.lower() == 'true':
            try:
                self.stock_codes, _ = read_stock_list_txt(stock_file)
                if not self.stock_codes:
                    self.logger.warning(f"股票代码文件 {stock_file} 为空，使用默认股票代码")
                    self.stock_codes = ['sh603288', 'sz000858']
            except FileNotFoundError:
                self.logger.error(f"找不到股票代码文件 {stock_file}，使用默认股票代码")
                self.stock_codes = ['sh603288', 'sz000858']
        else:
            self.stock_codes = stock_codes.split(',') if stock_codes else ['sh603288', 'sz000858']
    
    def start_requests(self):
        for stock_code in self.stock_codes:
            # 获取股票代码前缀对应的数字
            prefix = STOCK_PREFIX_MAP.get(stock_code[:2])
            if not prefix:
                self.logger.error(f"不支持的股票代码前缀: {stock_code}")
                continue
                
            # 构建API请求参数
            params = {
                'secid': f"{prefix}.{stock_code[2:]}",
                'fields': EASTMONEY_API['fields'],
                'ut': EASTMONEY_API['ut'],
                'wbp2u': EASTMONEY_API['wbp2u']
            }
            
            url = f"{EASTMONEY_API['base_url']}?" + "&".join([f"{k}={v}" for k, v in params.items()])
            
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={'stock_code': stock_code},
                headers=HEADERS
            )
    
    def parse(self, response):
        try:
            data = json.loads(response.text)
            if data.get('data'):
                stock_data = data['data']
                item = EastMoneyItem()
                item["stock_id"] = response.meta['stock_code']
                
                # 根据字段映射关系处理数据
                for api_field, item_field in FIELD_MAPPING.items():
                    value = stock_data.get(api_field)
                    if value is not None:
                        # 应用数据处理规则
                        divisor = FIELD_DIVISORS.get(item_field)
                        item[item_field] = value / divisor if divisor else value
                
                self._seen += 1
                if self._seen == 1 or self._seen % 200 == 0:
                    self.logger.info(f"已抓取 {self._seen} 条估值数据，最新: {item.get('stock_id')} - {item.get('stock_name')}")
                
                yield item
            else:
                self.logger.error(f"未获取到股票 {response.meta['stock_code']} 的数据")
                
        except Exception as e:
            self.logger.error(f"解析股票 {response.meta['stock_code']} 时出错: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
