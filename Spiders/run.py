from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from spiders.get_stock_list import StockListSpider
from scrapy import cmdline
from datetime import datetime, timedelta
import os
import sys
import subprocess

def run_stock_list_spider():
    """获取股票列表"""
    settings = get_project_settings()
    settings.set('REQUEST_FINGERPRINTER_IMPLEMENTATION', '2.7')
    process = CrawlerProcess(settings)
    process.crawl(StockListSpider, api_key='8371893ed4ab2b2f75b59c7fa26bf2fe')
    process.start()

def run_stock_detail_spider(stock_codes='sh603288,sz000858'):
    """爬取指定股票的详细信息"""
    cmdline.execute(f'scrapy crawl stock_detail -a stock_codes={stock_codes}'.split())

def run_stock_kline_spider_with_indicators(stock_codes):
    """获取带技术指标的K线数据"""
    cmdline.execute(f'scrapy crawl stock_kline -a use_file=true -a stock_codes={stock_codes} -a calc_indicators=true'.split())

# def run_stock_kline_spider_with_yesterday(stock_codes):
#     """获取昨天的K线数据"""
#     yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
#     cmdline.execute(f'scrapy crawl stock_kline -a use_file=true -a stock_codes={stock_codes} -a start_date={yesterday} -a end_date={yesterday} -a calc_indicators=true'.split())

def run_stock_kline_spider_with_yesterday(stock_codes):
    """获取昨天的K线数据"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    settings = get_project_settings()
    settings.set('REQUEST_FINGERPRINTER_IMPLEMENTATION', '2.7')
    process = CrawlerProcess(settings)
    process.crawl('stock_kline', 
                 use_file='true',
                 stock_codes=stock_codes,
                 start_date=yesterday,
                 end_date=yesterday,
                 calc_indicators='true')
    process.start()
    
def run_stock_kline_spider_without_indicators(stock_codes='sh603288'):
    """获取不带技术指标的K线数据"""
    cmdline.execute(f'scrapy crawl stock_kline -a stock_codes={stock_codes} -a calc_indicators=false'.split())

def upload_daily_report_to_cloudbase(report_date=None, log_file=None):
    """
    上传当天的信号分析报告到云数据库
    
    Args:
        report_date: 日期字符串，格式 YYYYMMDD，如果为None则使用当天日期
        log_file: 日志文件路径，用于记录上传过程
    """
    if report_date is None:
        report_date = datetime.now().strftime('%Y%m%d')
    
    def log(msg, also_print=True):
        if log_file:
            log_to_file(log_file, f"[UPLOAD] {msg}", also_print=also_print)
        elif also_print:
            print(msg)
    
    # 报告文件路径（在项目根目录）
    project_root = os.path.dirname(os.path.dirname(__file__))
    report_file = os.path.join(project_root, f'kdj_signals_{report_date}.txt')
    
    log(f"检查报告文件: {report_file}")
    
    # 检查文件是否存在
    if not os.path.exists(report_file):
        log(f"[WARNING] 报告文件不存在: {report_file}，跳过上传", also_print=False)
        return False
    
    log(f"报告文件存在，文件大小: {os.path.getsize(report_file)} 字节")
    
    # 上传脚本路径
    upload_script = os.path.join(project_root, 'Spiders', 'web', 'upload_daily_report_to_cloudbase.py')
    
    if not os.path.exists(upload_script):
        log(f"[ERROR] 上传脚本不存在: {upload_script}", also_print=False)
        return False
    
    log(f"上传脚本存在: {upload_script}")
    log(f"开始调用上传脚本，Python路径: {sys.executable}")
    
    try:
        # 调用上传脚本
        result = subprocess.run(
            [sys.executable, upload_script, '--file', report_file],
            cwd=project_root,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        
        log(f"上传脚本执行完成，退出码: {result.returncode}")
        
        if result.stdout:
            log(f"上传脚本输出:\n{result.stdout}", also_print=False)
        if result.stderr:
            log(f"上传脚本错误输出:\n{result.stderr}", also_print=False)
        
        if result.returncode == 0:
            log(f"[OK] 报告已成功上传到云数据库: {report_file}")
            return True
        else:
            log(f"[ERROR] 上传报告失败 (exit code {result.returncode}): {report_file}", also_print=False)
            return False
    except Exception as e:
        log(f"[ERROR] 上传报告时发生异常: {e}", also_print=False)
        import traceback
        error_trace = traceback.format_exc()
        log(f"[ERROR] 异常详情:\n{error_trace}", also_print=False)
        return False

# 股票代码列表
STOCK_CODES = (
    'sz001280'
    # 'sh688426,sh688217,sh605378,sh605277,sh605196,sh605155,sh605151,sh603990,'
    # 'sh603768,sh603725,sh603633,sh603363,sh603316,sh603307,sh603286,sh603193,'
    # 'sh603172,sh603163,sh603125,sh603121,sh603097,sh603081,sh603048,sh603045,'
    # 'sh603011,sh603003,sh601083,sh600860,sh600789,sh600778,sh600503,sh600356,'
    # 'sh600237,sh600202,sz301603,sz301591,sz301548,sz301538,sz301526,sz301512,'
    # 'sz301489,sz301459,sz301390,sz301362,sz301329,sz301328,sz301320,sz301279,'
    # 'sz301261,sz301252,sz301229,sz301215,sz301199,sz301196,sz301179,sz301135,'
    # 'sz301133,sz301127,sz301115,sz301112,sz301092,sz301072,sz301069,sz301005,'
    # 'sz301002,sz300982,sz300971,sz300964,sz300946,sz300943,sz300931,sz300922,'
    # 'sz300902,sz300879,sz300863,sz300838,sz300780,sz300767,sz300739,sz300549,'
    # 'sz300510,sz300508,sz300497,sz300477,sz300471,sz300400,sz300287,sz300232,'
    # 'sz300228,sz300199,sz300092,sz002997,sz002990,sz002965,sz002952,sz002929,'
    # 'sz002921,sz002917,sz002893,sz002750,sz002733,sz002697,sz002609,sz002560,'
    # 'sz002541,sz002536,sz002448,sz002406,sz002337,sz002334,sz002249,sz002166,'
    # 'sz002139,sz002123,sz002112,sz002052,sz002031,sz001380,sz001373,sz001326,'
    # 'sz001319,sz001287,sz001266,sz000897,sz000837,sz000700,sz000678,sz000597,'
    # 'sz000584,sz0836270,sh0688288,sh0688028,sh0603507,sh0603121,sh0603101,'
    # 'sh0603097,sh0600628,sh0600158,sz0301229,sz0301199,sz0301127,sz0300912,'
    # 'sz0300905,sz0300902,sz0300753,sz0300638,sz0300559,sz0300471,sz0300444,'
    # 'sz0300228,sz0300092,sz002997,sz002990,sz002965,sz002933,sz002860,sz001380'
)

def log_to_file(log_file, message, also_print=True):
    """将消息写入日志文件，同时可选地打印到控制台"""
    try:
        with open(log_file, 'a', encoding='utf-8') as f:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"[{timestamp}] {message}\n")
            f.flush()  # 立即刷新到磁盘
    except Exception as e:
        print(f"警告: 无法写入日志文件: {e}", file=sys.stderr)
    if also_print:
        print(message)

if __name__ == "__main__":
    # 清空日志文件（如果存在），实现每次启动覆盖
    log_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 'spiders.log')
    log_dir = os.path.dirname(log_file)
    
    # 确保日志目录存在
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        log_to_file(log_file, f"[INFO] 创建日志目录: {log_dir}", also_print=False)
    
    # 清空日志文件（覆盖模式）
    try:
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"=== 爬虫任务启动 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            f.write(f"工作目录: {os.getcwd()}\n")
            f.write(f"Python路径: {sys.executable}\n")
            f.write("=" * 80 + "\n\n")
    except Exception as e:
        # 如果无法写入日志文件，输出到stderr
        print(f"警告: 无法清空日志文件 {log_file}: {e}", file=sys.stderr)
    
    log_to_file(log_file, "[STEP 1] 日志文件初始化完成")
    
    # 运行获取股票列表的爬虫
    # run_stock_list_spider()
    
    # 运行获取股票详情的爬虫
    # run_stock_detail_spider()
    
    # 运行获取带技术指标K线数据的爬虫
    log_to_file(log_file, f"[STEP 2] 开始执行爬虫任务，股票代码数量: {len(STOCK_CODES.split(',')) if isinstance(STOCK_CODES, str) else len([c for c in STOCK_CODES.split(',') if c.strip()])}")
    try:
        run_stock_kline_spider_with_indicators(STOCK_CODES)
        log_to_file(log_file, "[STEP 2] 爬虫任务执行完成（正常退出）")
    except SystemExit as e:
        # cmdline.execute 可能会调用 sys.exit()，这是正常的
        log_to_file(log_file, f"[STEP 2] 爬虫任务执行完成（SystemExit，退出码: {e.code if hasattr(e, 'code') else 'N/A'}）")
    except Exception as e:
        log_to_file(log_file, f"[STEP 2] [ERROR] 爬虫任务执行出错: {e}", also_print=False)
        import traceback
        error_trace = traceback.format_exc()
        log_to_file(log_file, f"[STEP 2] [ERROR] 错误详情:\n{error_trace}", also_print=False)
        print(f"[ERROR] 爬虫任务执行出错: {e}", file=sys.stderr)
        traceback.print_exc()
    # run_stock_kline_spider_with_yesterday(STOCK_CODES)
    # 运行获取不带技术指标K线数据的爬虫
    # run_stock_kline_spider_without_indicators()

    # 爬虫运行完成后，上传当天的信号分析报告到云数据库
    # 使用 try-finally 确保上传逻辑一定会执行
    log_to_file(log_file, "[STEP 3] 开始上传信号分析报告到云数据库...")
    try:
        log_to_file(log_file, "=" * 80)
        log_to_file(log_file, "开始上传信号分析报告到云数据库...")
        log_to_file(log_file, "=" * 80)
        
        # 获取爬虫使用的日期（如果有 end_date 参数，使用该日期；否则使用当天）
        # 由于爬虫可能使用 end_date，我们需要从爬虫逻辑中获取，这里先使用当天日期
        # 如果爬虫使用了 end_date，可以在调用时传入
        report_date = datetime.now().strftime('%Y%m%d')
        log_to_file(log_file, f"[STEP 3] 准备上传报告日期: {report_date}")
        upload_success = upload_daily_report_to_cloudbase(report_date, log_file=log_file)
        
        if upload_success:
            log_to_file(log_file, "[STEP 3] [OK] 信号分析报告上传完成")
        else:
            log_to_file(log_file, "[STEP 3] [WARNING] 信号分析报告上传失败，请检查日志")
        
        log_to_file(log_file, "=" * 80)
        log_to_file(log_file, "[STEP 4] 所有任务执行完成")
    except Exception as e:
        log_to_file(log_file, f"[STEP 3] [ERROR] 上传报告时发生异常: {e}", also_print=False)
        import traceback
        error_trace = traceback.format_exc()
        log_to_file(log_file, f"[STEP 3] [ERROR] 错误详情:\n{error_trace}", also_print=False)
        print(f"[ERROR] 上传报告时发生异常: {e}", file=sys.stderr)
        traceback.print_exc()