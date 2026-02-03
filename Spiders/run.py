from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from spiders.get_stock_list import StockListSpider
from scrapy import cmdline
from datetime import datetime, timedelta
import os
import sys
import subprocess
import argparse

# 股票列表缓存配置
STOCK_LIST_FILE = 'stock_list.txt'
STOCK_LIST_CACHE_DAYS = 7  # 股票列表缓存天数（一周）


def is_stock_list_cache_valid(stock_file=STOCK_LIST_FILE, cache_days=STOCK_LIST_CACHE_DAYS):
    """
    检查股票列表缓存是否有效
    
    Args:
        stock_file: 股票列表文件路径
        cache_days: 缓存有效天数
        
    Returns:
        bool: 缓存是否有效
    """
    if not os.path.exists(stock_file):
        return False
    
    # 检查文件是否为空
    if os.path.getsize(stock_file) == 0:
        return False
    
    # 检查文件修改时间
    file_mtime = datetime.fromtimestamp(os.path.getmtime(stock_file))
    cache_expiry = datetime.now() - timedelta(days=cache_days)
    
    return file_mtime > cache_expiry


def run_stock_list_spider(force=False, log_file=None):
    """
    获取股票列表（带缓存）
    
    使用 subprocess 运行，避免 Twisted reactor 只能启动一次的问题
    
    Args:
        force: 是否强制刷新，忽略缓存
        log_file: 日志文件路径
    """
    def log(msg, also_print=True):
        if log_file:
            log_to_file(log_file, msg, also_print=also_print)
        elif also_print:
            print(msg)
    
    if not force and is_stock_list_cache_valid():
        log(f"[INFO] 股票列表缓存有效（{STOCK_LIST_CACHE_DAYS}天内），跳过获取")
        return False
    
    log(f"[INFO] 开始获取股票列表（使用子进程）...")
    
    # 获取当前脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    try:
        # 使用 subprocess 在单独进程中运行爬虫，避免 reactor 冲突
        result = subprocess.run(
            [sys.executable, '-c', '''
import sys
sys.path.insert(0, "{script_dir}")
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from spiders.get_stock_list import StockListSpider

settings = get_project_settings()
settings.set('REQUEST_FINGERPRINTER_IMPLEMENTATION', '2.7')
process = CrawlerProcess(settings)
process.crawl(StockListSpider, api_key='8371893ed4ab2b2f75b59c7fa26bf2fe')
process.start()
'''.format(script_dir=script_dir)],
            cwd=script_dir,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        
        if result.returncode == 0:
            log(f"[INFO] 股票列表获取完成")
            return True
        else:
            log(f"[WARNING] 股票列表获取失败 (exit code {result.returncode})")
            if result.stderr:
                log(f"[WARNING] 错误输出: {result.stderr}", also_print=False)
            return False
            
    except Exception as e:
        log(f"[ERROR] 获取股票列表时发生异常: {e}")
        import traceback
        log(f"[ERROR] 错误详情:\n{traceback.format_exc()}", also_print=False)
        return False

def run_stock_detail_spider(stock_codes='sh603288,sz000858'):
    """爬取指定股票的详细信息"""
    cmdline.execute(f'scrapy crawl stock_detail -a stock_codes={stock_codes}'.split())

def run_stock_kline_spider_with_indicators(stock_codes, target_date=None):
    """
    获取带技术指标的K线数据
    
    Args:
        stock_codes: 股票代码
        target_date: 目标日期，格式 YYYYMMDD，如果为None则使用今天
    """
    if target_date:
        # 指定日期时使用 CrawlerProcess
        settings = get_project_settings()
        settings.set('REQUEST_FINGERPRINTER_IMPLEMENTATION', '2.7')
        process = CrawlerProcess(settings)
        process.crawl('stock_kline', 
                     use_file='true',
                     stock_codes=stock_codes,
                     start_date=target_date,
                     end_date=target_date,
                     calc_indicators='true')
        process.start()
    else:
        # 默认今天，使用 cmdline
        cmdline.execute(f'scrapy crawl stock_kline -a use_file=true -a stock_codes={stock_codes} -a calc_indicators=true'.split())

# def run_stock_kline_spider_with_yesterday(stock_codes):
#     """获取昨天的K线数据"""
#     yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
#     cmdline.execute(f'scrapy crawl stock_kline -a use_file=true -a stock_codes={stock_codes} -a start_date={yesterday} -a end_date={yesterday} -a calc_indicators=true'.split())

def run_stock_kline_spider_with_yesterday(stock_codes):
    """获取昨天的K线数据（已废弃，请使用 run_stock_kline_spider_with_indicators 并指定日期）"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    run_stock_kline_spider_with_indicators(stock_codes, target_date=yesterday)
    
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

def validate_date(date_str):
    """
    验证日期字符串格式
    
    Args:
        date_str: 日期字符串，格式应为 YYYYMMDD
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if not date_str:
        return False, "日期字符串不能为空"
    
    if len(date_str) != 8:
        return False, f"日期格式错误：应为 YYYYMMDD 格式（8位数字），实际为 {len(date_str)} 位"
    
    if not date_str.isdigit():
        return False, f"日期格式错误：应全部为数字，实际为: {date_str}"
    
    try:
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        
        # 验证日期是否有效
        datetime(year, month, day)
        
        return True, None
    except ValueError as e:
        return False, f"日期无效：{e}"

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='股票数据爬虫脚本')
    parser.add_argument('--date', type=str, help='指定运行日期，格式：YYYYMMDD（例如：20240101）。如果不指定，默认运行今天的数据')
    parser.add_argument('--yesterday', action='store_true', help='运行昨天的数据（与 --date 参数互斥，如果同时指定，--date 优先）')
    args = parser.parse_args()
    
    # 根据参数确定运行日期
    date_specified = False  # 标记是否明确指定了日期
    if args.date:
        # 验证日期格式
        is_valid, error_msg = validate_date(args.date)
        if not is_valid:
            print(f"错误: {error_msg}", file=sys.stderr)
            sys.exit(1)
        target_date = args.date
        date_desc = f"指定日期 ({target_date})"
        date_specified = True
    elif args.yesterday:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        date_desc = "昨天"
        date_specified = True
    else:
        target_date = datetime.now().strftime('%Y%m%d')
        date_desc = "今天"
        date_specified = False
    
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
            f.write(f"运行模式: {date_desc}的数据 ({target_date})\n")
            f.write("=" * 80 + "\n\n")
    except Exception as e:
        # 如果无法写入日志文件，输出到stderr
        print(f"警告: 无法清空日志文件 {log_file}: {e}", file=sys.stderr)
    
    log_to_file(log_file, f"[STEP 1] 日志文件初始化完成，运行模式: {date_desc}的数据 ({target_date})")
    
    # 检查并更新股票列表（一周获取一次）
    log_to_file(log_file, f"[STEP 1.5] 检查股票列表缓存...")
    stock_file_path = os.path.join(os.path.dirname(__file__), STOCK_LIST_FILE)
    if is_stock_list_cache_valid(stock_file_path):
        file_mtime = datetime.fromtimestamp(os.path.getmtime(stock_file_path))
        log_to_file(log_file, f"[STEP 1.5] 股票列表缓存有效，上次更新: {file_mtime.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        log_to_file(log_file, f"[STEP 1.5] 股票列表缓存已过期或不存在，开始更新...")
        run_stock_list_spider(force=False, log_file=log_file)
    
    # 运行获取股票详情的爬虫
    # run_stock_detail_spider()
    
    # 运行获取带技术指标K线数据的爬虫
    log_to_file(log_file, f"[STEP 2] 开始执行爬虫任务，股票代码数量: {len(STOCK_CODES.split(',')) if isinstance(STOCK_CODES, str) else len([c for c in STOCK_CODES.split(',') if c.strip()])}，日期: {target_date}")
    try:
        # 如果明确指定了日期参数，传入日期参数；否则使用默认（今天）
        if date_specified:
            run_stock_kline_spider_with_indicators(STOCK_CODES, target_date=target_date)
        else:
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
    # 运行获取不带技术指标K线数据的爬虫
    # run_stock_kline_spider_without_indicators()

    # 爬虫运行完成后，上传信号分析报告到云数据库
    # 使用 try-finally 确保上传逻辑一定会执行
    log_to_file(log_file, f"[STEP 3] 开始上传{date_desc}的信号分析报告到云数据库...")
    try:
        log_to_file(log_file, "=" * 80)
        log_to_file(log_file, f"开始上传{date_desc}的信号分析报告到云数据库...")
        log_to_file(log_file, "=" * 80)
        
        # 使用目标日期上传报告
        log_to_file(log_file, f"[STEP 3] 准备上传报告日期: {target_date}")
        upload_success = upload_daily_report_to_cloudbase(target_date, log_file=log_file)
        
        if upload_success:
            log_to_file(log_file, f"[STEP 3] [OK] {date_desc}的信号分析报告上传完成")
        else:
            log_to_file(log_file, f"[STEP 3] [WARNING] {date_desc}的信号分析报告上传失败，请检查日志")
        
        log_to_file(log_file, "=" * 80)
        log_to_file(log_file, "[STEP 4] 所有任务执行完成")
    except Exception as e:
        log_to_file(log_file, f"[STEP 3] [ERROR] 上传报告时发生异常: {e}", also_print=False)
        import traceback
        error_trace = traceback.format_exc()
        log_to_file(log_file, f"[STEP 3] [ERROR] 错误详情:\n{error_trace}", also_print=False)
        print(f"[ERROR] 上传报告时发生异常: {e}", file=sys.stderr)
        traceback.print_exc()