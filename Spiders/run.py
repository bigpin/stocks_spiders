from datetime import datetime, timedelta
import os
import sys
import subprocess
import threading
import time
import argparse

# 股票列表缓存配置
STOCK_LIST_FILE = 'stock_list.txt'
STOCK_LIST_CACHE_DAYS = 7  # 股票列表缓存天数（一周）
# 股票估值缓存配置
STOCK_DETAIL_FILE = 'stock_detail_data.csv'
STOCK_DETAIL_CACHE_DAYS = 1  # 估值缓存天数（默认1天）
STOCK_DETAIL_REFRESH_HOUR = 15
STOCK_DETAIL_REFRESH_MINUTE = 30


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

    优先使用 baostock 获取，失败时返回 False。

    Args:
        force: 是否强制刷新，忽略缓存
        log_file: 日志文件路径
    """
    def log(msg, also_print=True):
        if log_file:
            log_to_file(log_file, msg, also_print=also_print)
        elif also_print:
            print(msg)
    
    # stock_list.txt 在项目根目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    stock_file_path = os.path.join(project_root, STOCK_LIST_FILE)
    
    if not force and is_stock_list_cache_valid(stock_file_path):
        log(f"[INFO] 股票列表缓存有效（{STOCK_LIST_CACHE_DAYS}天内），跳过获取")
        return False
    
    log(f"[INFO] 股票列表文件路径: {stock_file_path}")
    
    # 优先使用 baostock 获取股票列表（无需 API Key、无需子进程）
    try:
        from spiders.baostock_helper import get_stock_list_baostock_entries
        entries = get_stock_list_baostock_entries(a_share_only=True)
        if entries:
            with open(stock_file_path, 'w', encoding='utf-8') as f:
                for code, nm in entries:
                    if nm:
                        f.write(f"{code}\t{nm}\n")
                    else:
                        f.write(f"{code}\n")
            log(f"[INFO] 股票列表获取完成（baostock），共 {len(entries)} 只")
            return True
        log(f"[WARNING] baostock 返回空列表（可能为非交易日）")
    except Exception as e:
        log(f"[WARNING] baostock 获取股票列表失败: {e}", also_print=False)
    return False

def is_stock_detail_cache_valid(detail_file=STOCK_DETAIL_FILE, cache_days=STOCK_DETAIL_CACHE_DAYS):
    """
    检查估值缓存是否有效
    """
    if not os.path.exists(detail_file):
        return False
    if os.path.getsize(detail_file) == 0:
        return False
    file_mtime = datetime.fromtimestamp(os.path.getmtime(detail_file))
    cache_expiry = datetime.now() - timedelta(days=cache_days)
    return file_mtime > cache_expiry

def should_refresh_stock_detail_cache(detail_file=STOCK_DETAIL_FILE, min_rows=4000):
    """
    收盘后固定刷新估值缓存：
    - 文件不存在/为空：立即刷新
    - 文件行数 < min_rows（不完整）：立即刷新
    - 仅在 15:30 以后，且文件不是今天更新的情况下刷新
    """
    if not os.path.exists(detail_file) or os.path.getsize(detail_file) == 0:
        return True
    # 行数过少说明是残缺文件，强制刷新
    try:
        with open(detail_file, 'r', encoding='utf-8') as _f:
            row_count = sum(1 for _ in _f)
        if row_count < min_rows:
            return True
    except Exception:
        return True
    now = datetime.now()
    file_mtime = datetime.fromtimestamp(os.path.getmtime(detail_file))
    cache_expiry = now - timedelta(days=STOCK_DETAIL_CACHE_DAYS)
    if file_mtime < cache_expiry:
        return True
    after_close = (now.hour > STOCK_DETAIL_REFRESH_HOUR or
                   (now.hour == STOCK_DETAIL_REFRESH_HOUR and now.minute >= STOCK_DETAIL_REFRESH_MINUTE))
    if not after_close:
        return False
    return file_mtime.date() != now.date()

def _stock_detail_worker_init(script_dir):
    """ProcessPoolExecutor 子进程初始化：将 Spiders/ 加入 sys.path，确保 baostock_helper 可导入。"""
    import sys as _sys
    if script_dir not in _sys.path:
        _sys.path.insert(0, script_dir)


def run_stock_detail_spider(stock_file_path, log_file=None, target_date=None):
    """用 baostock 批量获取全市场估值数据（K线 + PE），写入 stock_detail_data.csv。"""
    import csv as csv_module
    from concurrent.futures import ProcessPoolExecutor, wait

    def log(msg, also_print=True):
        if log_file:
            log_to_file(log_file, msg, also_print=also_print)
        elif also_print:
            print(msg)

    script_dir   = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_file_path = os.path.join(project_root, STOCK_DETAIL_FILE)

    # 读取股票列表（兼容「代码」与「代码\\t名称」，只取代码列）
    codes = []
    try:
        if os.path.exists(stock_file_path):
            from spiders.baostock_helper import read_stock_list_txt
            codes, _ = read_stock_list_txt(stock_file_path)
            log(f"[INFO] 估值抓取股票数量: {len(codes)}")
        else:
            log(f"[WARNING] 股票列表不存在: {stock_file_path}")
            return False
    except Exception as e:
        log(f"[WARNING] 读取股票列表失败: {e}")
        return False

    if not codes:
        log("[WARNING] 股票列表为空")
        return False

    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)

    from spiders.baostock_helper import fetch_stock_fundamental_worker

    CSV_FIELDS = [
        'stock_id', 'stock_name', 'new_price', 'percentage_change', 'price_change',
        'trading_volume', 'trading_value', 'highest_price', 'lowest_price',
        'opening_price', 'closing_price', 'turnover_rate', 'pe', 'pb',
    ]
    FLUSH_EVERY = 200  # 每积攒 200 条写一次磁盘

    def flush_rows(rows, file_obj, writer):
        for r in rows:
            writer.writerow([r.get(f, '') for f in CSV_FIELDS])
        file_obj.flush()

    start_time    = time.time()
    total_ok      = 0
    failed        = 0
    done_count    = 0
    last_log_time = start_time
    pending       = []   # 待写缓冲

    try:
        # 写入模式：先写表头，之后追加
        out_f  = open(output_file_path, 'w', encoding='utf-8', newline='')
        writer = csv_module.writer(out_f)
        writer.writerow(CSV_FIELDS)
        out_f.flush()
    except Exception as e:
        log(f"[ERROR] 创建估值数据文件失败: {e}")
        return False

    try:
        with ProcessPoolExecutor(
            max_workers=8,
            initializer=_stock_detail_worker_init,
            initargs=(script_dir,),
        ) as executor:
            futures = {
                executor.submit(fetch_stock_fundamental_worker, code, target_date): code
                for code in codes
            }
            pending_futures = set(futures.keys())
            wait_timeout = 120
            while pending_futures:
                done_set, pending_futures = wait(pending_futures, timeout=wait_timeout)
                if not done_set:
                    log(f"[WARNING] 存在估值任务超时未返回（>{wait_timeout}s），继续等待下一批结果")
                    continue
                for future in done_set:
                    try:
                        result = future.result(timeout=0)
                        if result:
                            pending.append(result)
                            total_ok += 1
                        else:
                            failed += 1
                    except Exception:
                        failed += 1

                    done_count += 1

                # 每积攒 FLUSH_EVERY 条写一次磁盘
                if len(pending) >= FLUSH_EVERY:
                    flush_rows(pending, out_f, writer)
                    pending.clear()

                now = time.time()
                if now - last_log_time >= 30:
                    elapsed = int(now - start_time)
                    log(f"[INFO] 估值抓取进度: {done_count}/{len(codes)}，已落盘 {total_ok} 只，已用时 {elapsed}s")
                    last_log_time = now

    except Exception as e:
        log(f"[ERROR] 估值批量抓取异常: {e}")
        import traceback
        log(f"[ERROR] 错误详情:\n{traceback.format_exc()}", also_print=False)
    finally:
        # 剩余数据落盘，关闭文件
        if pending:
            flush_rows(pending, out_f, writer)
        out_f.close()

    elapsed = int(time.time() - start_time)
    if total_ok == 0:
        log("[WARNING] 估值数据抓取结果为空")
        return False

    log(f"[INFO] 估值数据写入完成: 成功 {total_ok} 只，失败 {failed} 只，耗时 {elapsed}s")
    log(f"[INFO] 估值文件路径: {output_file_path}")
    return True

def run_stock_kline_spider_with_indicators(stock_codes, target_date=None, stock_file_path=None):
    """
    获取带技术指标的K线数据

    Args:
        stock_codes: 股票代码
        target_date: 目标日期，格式 YYYYMMDD，如果为None则使用今天
        stock_file_path: 股票列表文件路径
    """
    from spiders.stock_kline import StockKlineSpider

    kwargs = dict(
        use_file='true',
        stock_codes=stock_codes,
        calc_indicators=True,
    )
    if target_date:
        kwargs['start_date'] = target_date
        kwargs['end_date'] = target_date
    if stock_file_path:
        kwargs['stock_file'] = stock_file_path

    spider = StockKlineSpider(**kwargs)
    try:
        spider.run()
    finally:
        spider.cleanup()

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
    
    # 上传脚本路径（已移动到 scripts/cloud 目录）
    upload_script = os.path.join(project_root, 'scripts', 'cloud', 'upload_report.py')
    
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


def cleanup_old_daily_prices(days=30, log_file=None):
    """
    清理本地 SQLite 中超过 N 天的日线价格数据

    删除 insert_date 超过 days 天的信号关联的 stock_signal_daily_prices 记录。
    """
    import sqlite3

    def log(msg, also_print=True):
        if log_file:
            log_to_file(log_file, f"[CLEANUP] {msg}", also_print=also_print)
        elif also_print:
            print(msg)

    project_root = os.path.dirname(os.path.dirname(__file__))
    db_path = os.path.join(project_root, 'stock_signals.db')

    if not os.path.exists(db_path):
        log(f"[WARNING] 数据库不存在: {db_path}，跳过清理")
        return False

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 先统计待删除数量
        cursor.execute(f"""
            SELECT COUNT(*) FROM stock_signal_daily_prices
            WHERE signal_id IN (
                SELECT id FROM stock_signals WHERE insert_date < date('now', '-{days} days')
            )
        """)
        count = cursor.fetchone()[0]

        if count == 0:
            log(f"无超过 {days} 天的日线数据需要清理")
            conn.close()
            return True

        log(f"清理超过 {days} 天的日线数据，共 {count} 条...")

        cursor.execute(f"""
            DELETE FROM stock_signal_daily_prices
            WHERE signal_id IN (
                SELECT id FROM stock_signals WHERE insert_date < date('now', '-{days} days')
            )
        """)
        deleted = cursor.rowcount
        conn.commit()
        conn.close()

        log(f"[OK] 已清理 {deleted} 条过期日线数据")
        return True
    except Exception as e:
        log(f"[ERROR] 清理失败: {e}")
        return False


def sync_sqlite_to_cloud(log_file=None):
    """
    同步本地 SQLite 数据库到微信云开发数据库
    """
    def log(msg, also_print=True):
        if log_file:
            log_to_file(log_file, f"[SYNC] {msg}", also_print=also_print)
        elif also_print:
            print(msg)

    project_root = os.path.dirname(os.path.dirname(__file__))
    sync_script = os.path.join(project_root, 'scripts', 'cloud', 'sync_sqlite_to_cloud.py')

    if not os.path.exists(sync_script):
        log(f"[WARNING] 同步脚本不存在: {sync_script}，跳过同步", also_print=False)
        return False

    log(f"开始同步 SQLite 到云数据库...")

    try:
        result = subprocess.run(
            [sys.executable, sync_script, '--incremental', '--verbose'],
            cwd=project_root,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )

        log(f"同步脚本执行完成，退出码: {result.returncode}")

        if result.stdout:
            log(f"同步脚本输出:\n{result.stdout}", also_print=False)
        if result.stderr:
            log(f"同步脚本错误输出:\n{result.stderr}", also_print=False)

        if result.returncode == 0:
            log("[OK] SQLite 已同步到云数据库")
            return True
        else:
            log(f"[WARNING] 同步失败 (exit code {result.returncode})", also_print=False)
            return False
    except Exception as e:
        log(f"[ERROR] 同步时发生异常: {e}", also_print=False)
        import traceback
        log(f"[ERROR] 异常详情:\n{traceback.format_exc()}", also_print=False)
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


def wait_for_network(log_file=None, timeout=5, check_interval=5, max_checks=120):
    """
    唤醒后等网络恢复再继续执行。
    避免 launchd 定时任务在系统刚醒来时卡在网络请求。
    """
    import urllib.request
    # 绕过系统代理直连测试，避免代理软件未运行时误判网络不通
    proxy_handler = urllib.request.ProxyHandler({})
    opener = urllib.request.build_opener(proxy_handler)
    test_urls = [
        "https://www.baidu.com/",
        "http://connectivitycheck.platform.hicloud.com/generate_204",
        "https://www.qq.com/",
    ]
    message = "[STEP 0.9] 网络状态检查：等待网络恢复"
    if log_file:
        log_to_file(log_file, message)

    for i in range(1, max_checks + 1):
        for url in test_urls:
            try:
                opener.open(url, timeout=timeout)
                if log_file:
                    log_to_file(log_file, "[STEP 0.9] 网络已恢复")
                return True
            except Exception:
                continue

        if log_file:
            log_to_file(
                log_file,
                f"[STEP 0.9] 网络仍未就绪，{check_interval}秒后重试 ({i}/{max_checks})",
            )
        time.sleep(check_interval)

    if log_file:
        log_to_file(log_file, "[STEP 0.9] 等待网络超时，继续尝试执行任务")
    return False


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


_lock_fd = None  # 保持锁文件描述符打开，确保 fcntl.flock 在进程生命周期内持续生效


def _acquire_pid_lock(lock_path):
    """尝试获取 PID 锁文件，防止并发执行。返回 True 表示成功获取锁。"""
    global _lock_fd
    import fcntl
    try:
        # 先尝试以排他方式创建/打开锁文件
        fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            os.close(fd)
            return False
        # 获取锁成功，写入 PID 并保持 fd 打开
        os.write(fd, str(os.getpid()).encode())
        os.fsync(fd)
        _lock_fd = fd
        return True
    except (IOError, OSError):
        return False


def _release_pid_lock(lock_path):
    """释放锁文件并删除。"""
    global _lock_fd
    try:
        if _lock_fd is not None:
            import fcntl
            fcntl.flock(_lock_fd, fcntl.LOCK_UN)
            os.close(_lock_fd)
            _lock_fd = None
    except OSError:
        pass
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
    except OSError:
        pass


if __name__ == "__main__":
    # 防止并发执行：通过 PID 锁文件互斥
    _pid_lock_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.spiders.pid.lock')
    if not _acquire_pid_lock(_pid_lock_path):
        print(f"另一个爬虫进程正在运行，退出当前实例 (PID: {os.getpid()})", file=sys.stderr)
        sys.exit(0)

    import atexit
    atexit.register(_release_pid_lock, _pid_lock_path)

    # 轮转 launchd 外层日志，避免 /tmp/spiders.launchd.test.log 无限增长
    _launchd_log = "/tmp/spiders.launchd.test.log"
    _max_bytes = 20 * 1024 * 1024
    _keep = 3
    try:
        if os.path.exists(_launchd_log) and os.path.getsize(_launchd_log) > _max_bytes:
            for i in range(_keep, 1, -1):
                src = f"{_launchd_log}.{i-1}"
                dst = f"{_launchd_log}.{i}"
                if os.path.exists(src):
                    os.replace(src, dst)
            os.replace(_launchd_log, f"{_launchd_log}.1")
            with open(_launchd_log, "w", encoding="utf-8") as f:
                f.write("")
    except Exception:
        pass

    # 工作目录需为项目根；launchd 的 WorkingDirectory 若受限，此处兜底
    _project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    try:
        os.chdir(_project_root)
    except OSError as e:
        print(f"警告: 无法切换到项目根目录 {_project_root}: {e}", file=sys.stderr)

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

    # 先等网络恢复，避免唤醒后长时间卡在网络调用
    wait_for_network(log_file=log_file, max_checks=60)

    # 检查并更新股票列表（一周获取一次）
    log_to_file(log_file, f"[STEP 1.5] 检查股票列表缓存...")
    # stock_list.txt 在项目根目录（run.py 的上一级目录）
    project_root = os.path.dirname(os.path.dirname(__file__))
    stock_file_path = os.path.join(project_root, STOCK_LIST_FILE)
    if is_stock_list_cache_valid(stock_file_path):
        file_mtime = datetime.fromtimestamp(os.path.getmtime(stock_file_path))
        log_to_file(log_file, f"[STEP 1.5] 股票列表缓存有效，上次更新: {file_mtime.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        log_to_file(log_file, f"[STEP 1.5] 股票列表缓存已过期或不存在，开始更新...")
        run_stock_list_spider(force=False, log_file=log_file)
    
    # 估值数据已合并到 K 线阶段（baostock peTTM/pbMRQ），不再单独抓取
    detail_file_path = os.path.join(project_root, STOCK_DETAIL_FILE)
    log_to_file(log_file, f"[STEP 1.7] 估值数据将在 K 线阶段从 peTTM/pbMRQ 字段自动生成，跳过独立估值抓取")
    
    # 统计本次将要处理的股票代码数量（优先从股票列表文件统计）
    try:
        if os.path.exists(stock_file_path):
            from spiders.baostock_helper import read_stock_list_txt
            codes, _ = read_stock_list_txt(stock_file_path)
            code_count = len(codes)
        else:
            if isinstance(STOCK_CODES, str):
                code_count = len([c for c in STOCK_CODES.split(',') if c.strip()])
            else:
                code_count = len(STOCK_CODES)
    except Exception as e:
        # 出现异常时退回用 STOCK_CODES 粗略统计，避免影响主流程
        log_to_file(log_file, f"[STEP 1.5] 统计股票数量时出错: {e}，退回使用 STOCK_CODES 统计", also_print=False)
        if isinstance(STOCK_CODES, str):
            code_count = len([c for c in STOCK_CODES.split(',') if c.strip()])
        else:
            code_count = len(STOCK_CODES)
    
    # 运行获取带技术指标K线数据的爬虫
    log_to_file(log_file, f"[STEP 2] 开始执行爬虫任务，股票代码数量: {code_count}，日期: {target_date}")
    try:
        # 如果明确指定了日期参数，传入日期参数；否则使用默认（今天）
        if date_specified:
            run_stock_kline_spider_with_indicators(STOCK_CODES, target_date=target_date, stock_file_path=stock_file_path)
        else:
            run_stock_kline_spider_with_indicators(STOCK_CODES, stock_file_path=stock_file_path)
        log_to_file(log_file, "[STEP 2] 爬虫任务执行完成（正常退出）")
    except SystemExit as e:
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

    # 清理超过30天的日线价格数据
    log_to_file(log_file, "[STEP 2.5] 清理过期日线数据...")
    try:
        cleanup_old_daily_prices(days=30, log_file=log_file)
    except Exception as e:
        log_to_file(log_file, f"[STEP 2.5] [WARNING] 清理异常: {e}", also_print=False)

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
        log_to_file(log_file, "[STEP 4] 开始同步 SQLite 到云数据库...")
        try:
            sync_success = sync_sqlite_to_cloud(log_file=log_file)
            if sync_success:
                log_to_file(log_file, f"[STEP 4] [OK] SQLite 同步完成")
            else:
                log_to_file(log_file, f"[STEP 4] [WARNING] SQLite 同步失败，请检查日志")
        except Exception as sync_e:
            log_to_file(log_file, f"[STEP 4] [ERROR] 同步异常: {sync_e}", also_print=False)

        log_to_file(log_file, "=" * 80)
        log_to_file(log_file, "[STEP 5] 所有任务执行完成")
    except Exception as e:
        log_to_file(log_file, f"[STEP 3] [ERROR] 上传报告时发生异常: {e}", also_print=False)
        import traceback
        error_trace = traceback.format_exc()
        log_to_file(log_file, f"[STEP 3] [ERROR] 错误详情:\n{error_trace}", also_print=False)
        print(f"[ERROR] 上传报告时发生异常: {e}", file=sys.stderr)
        traceback.print_exc()
import threading
