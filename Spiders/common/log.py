# -*- coding: utf-8 -*-
"""统一日志模块。

控制台输出使用 rich 彩色分级（RichHandler），文件输出使用纯文本写入 logs/spiders.log。
rich 不可用时自动降级为普通 StreamHandler，不会导致程序崩溃。

用法：
    from common.log import get_logger, get_fileonly_logger

    log = get_logger(__name__)        # 同时输出到控制台(彩色)和文件
    log.info("普通信息")
    log.warning("警告")
    log.error("错误")

    flog = get_fileonly_logger()      # 仅写入文件，不输出到控制台（用于 also_print=False 场景）
    flog.info("只写文件不打印")
"""
import logging
import os
import sys
import time

try:
    from rich.logging import RichHandler
    from rich.traceback import install as _install_rich_traceback
    from rich.theme import Theme
    _RICH_AVAILABLE = True
except Exception:  # rich 未安装时降级
    _RICH_AVAILABLE = False

# 项目根目录：Spiders/common/log.py -> 上溯两级 -> 项目根
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_LOG_FILE = os.path.join(_PROJECT_ROOT, 'logs', 'spiders.log')

_FILE_FORMATTER = logging.Formatter(
    '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# PinnedProgress 滚动日志专用：不显示 logger 名，避免刷屏
_CONSOLE_FORMATTER = logging.Formatter(
    '[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

_root_configured = set()
_fileonly_configured = {}


def _make_file_handler(log_file):
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(_FILE_FORMATTER)
    return fh


def _make_console_handler():
    if _RICH_AVAILABLE:
        from rich.console import Console
        # 自定义日志级别颜色；同时禁用默认 Highlighter，避免消息里的 [STEP 1]/数字/括号被额外着色
        theme = Theme({
            'logging.level.debug': 'dim',
            'logging.level.info': 'bold green',
            'logging.level.warning': 'bold yellow',
            'logging.level.error': 'bold red',
            'logging.level.critical': 'bold reverse red',
        })
        # 在非 TTY（被其他工具捕获 stdout）时，默认宽度只有 80，长中文消息会被错误换行。
        # 这里把非 TTY 宽度设为 200，避免一条日志被拆成多行。
        console_kwargs = {'theme': theme}
        if not sys.stdout.isatty():
            console_kwargs['width'] = 200
            console_kwargs['soft_wrap'] = False
        return RichHandler(
            console=Console(**console_kwargs),
            show_time=True,
            show_level=True,
            show_path=False,
            markup=False,
            rich_tracebacks=True,
            highlighter=None,
        )
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(_FILE_FORMATTER)
    return ch


def _configure_root(log_file):
    """配置 root logger（控制台 + 文件），每个 log_file 只配置一次。"""
    if log_file in _root_configured:
        return
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(_make_file_handler(log_file))
    root.addHandler(_make_console_handler())
    if _RICH_AVAILABLE:
        try:
            _install_rich_traceback()
        except Exception:
            pass
    _root_configured.add(log_file)


def get_logger(name='spiders', log_file=None):
    """获取同时输出到控制台(彩色)和文件的 logger。"""
    log_file = log_file or DEFAULT_LOG_FILE
    _configure_root(log_file)
    return logging.getLogger(name)


def get_fileonly_logger(log_file=None):
    """获取仅写入文件、不输出到控制台的 logger（用于 also_print=False 场景）。"""
    log_file = log_file or DEFAULT_LOG_FILE
    if log_file in _fileonly_configured:
        return _fileonly_configured[log_file]
    # 用独立 logger + propagate=False，避免继承 root 的 RichHandler 打印到控制台
    logger = logging.getLogger('fileonly.%s' % os.path.basename(log_file))
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(_make_file_handler(log_file))
    _fileonly_configured[log_file] = logger
    return logger


# ============================================================
# PinnedProgress：进度条置顶 + 下方滚动日志
# rich 默认把进度条放在「底部」，日志在上方；本类反其道而行——
# 用 Live + Group(进度条, 日志面板) 把进度条钉在顶部，日志缓冲滚在下面。
# ============================================================

if _RICH_AVAILABLE:
    from collections import deque

    from rich.console import Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        Progress,
        ProgressColumn,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.text import Text

    class RatePerMinuteColumn(ProgressColumn):
        """自定义 rich 进度条列：显示每分钟处理数量（只/分）。"""

        def render(self, task):
            speed = task.speed
            if speed is None or speed <= 0:
                return Text("? 只/分", style="progress.data.speed")
            return Text(f"{speed * 60:.0f} 只/分", style="progress.data.speed")

    _LEVEL_STYLE = {
        'DEBUG': 'dim',
        'INFO': 'white',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold red',
    }

    class _DynamicPanel:
        """随终端高度动态占满剩余空间的 Panel。

        关键：高度不在构造时写死，而是在每次渲染时读取当前 console 尺寸计算，
        因此终端缩放（SIGWINCH）后 rich 的 Live 自动重绘时，框高会跟着变。
        """

        def __init__(self, renderable, title='日志', border_style='cyan'):
            self.renderable = renderable
            self.title = title
            self.border_style = border_style

        def __rich_console__(self, console, options):
            term_height = console.size.height or 24
            # 进度条约 2 行、Panel 上下边框 2 行、余量 1 行
            panel_height = max(8, term_height - 5)
            panel = Panel(
                self.renderable,
                title=self.title,
                border_style=self.border_style,
                expand=True,
                height=panel_height,
            )
            yield panel

    class _PinnedLogHandler(logging.Handler):
        """把 logging 记录写进 PinnedProgress 的滚动日志缓冲。"""

        def __init__(self, pinned):
            super().__init__()
            self._pinned = pinned
            self.setFormatter(_CONSOLE_FORMATTER)

        def emit(self, record):
            try:
                msg = self.format(record)
                self._pinned.log(msg, style=_LEVEL_STYLE.get(record.levelname))
            except Exception:
                # 滚动日志 handler 不要阻塞业务；但出问题时保留 stderr 痕迹，便于排查
                import traceback
                traceback.print_exc()

    class PinnedProgress:
        """进度条 + 滚动日志的富文本控制台组件。

        默认 pin='bottom'（进度条在底部、日志在上方滚动），即 rich 的常规观感；
        传入 pin='top' 则进度条置顶、日志在下方滚动。

        用法：
            pp = PinnedProgress("回填股价")          # 默认进度条在底部
            pp.start(logger)            # 接管 logger 的 console 输出（避免重复打印）
            task = pp.add_task("抓取中", total=100)
            for i in range(100):
                ...
                pp.update(task, advance=1)
                pp.log("已处理 %d" % i)   # 或者直接用 logger.info，会进滚动日志区
            pp.stop()
            # 推荐 with 写法：
            with PinnedProgress("回填股价").bind(logger) as pp:
                ...
        """

        def __init__(self, title='进度', max_log_lines=None, console=None, pin='bottom'):
            from rich.console import Console
            self.console = console or Console()
            self.pin = pin
            self.progress = Progress(
                SpinnerColumn(),
                TextColumn('[progress.description]{task.description}'),
                BarColumn(),
                TaskProgressColumn(),
                RatePerMinuteColumn(),
                TimeElapsedColumn(),
                console=self.console,
                expand=True,
            )
            # 日志缓冲区给一个充足的固定上限即可；实际显示行数由 _DynamicPanel
            # 按当前终端高度动态裁剪，因此无需在构造时依赖终端高度。
            if max_log_lines is None:
                max_log_lines = 80
            self._buf = deque(maxlen=max_log_lines)
            # 渲染时按当前终端高度动态占满剩余空间的日志面板
            self._panel = _DynamicPanel(Group(), title='日志', border_style='cyan')
            # pin='bottom' -> Group(日志, 进度条)：进度条钉在底部（rich 默认观感）
            # pin='top'    -> Group(进度条, 日志)：进度条置顶、日志在下方滚动
            if pin == 'bottom':
                layout = Group(self._panel, self.progress)
            else:
                layout = Group(self.progress, self._panel)
            self._live = Live(
                layout,
                console=self.console,
                refresh_per_second=8,
                screen=True,  # 进入全屏 alternate buffer，避免旧帧重影
            )
            self._handler = _PinnedLogHandler(self)
            self._removed = []   # (logger, handler) 被临时摘掉的 RichHandler
            self._started = False
            # 去重：避免同一消息因 handler 重复挂载/多 logger 而在短时间内刷屏
            self._last_log_msg = None
            self._last_log_time = 0.0
            self._log_dedup_window = 1.5  # 秒

        def start(self, logger=None):
            """启动 Live；若传入 logger，则接管其 console 输出（摘掉 RichHandler，
            改由本组件的滚动日志区承接，避免同一行日志既滚又出现在别处）。"""
            if self._started:
                return self
            self._live.start()
            self._started = True
            # 从指定 logger 和 root 上摘掉 RichHandler（子 logger 默认 propagate 到 root）
            for lg in ([logger] if logger is not None else []) + [logging.getLogger()]:
                for h in list(lg.handlers):
                    if isinstance(h, RichHandler):
                        lg.removeHandler(h)
                        self._removed.append((lg, h))
            # 滚动日志 handler 只加到 root，避免在子 logger 上重复打印
            root = logging.getLogger()
            if not any(isinstance(x, _PinnedLogHandler) for x in root.handlers):
                root.addHandler(self._handler)
            return self

        def bind(self, logger):
            """语法糖：with PinnedProgress('x').bind(logger) as pp: ..."""
            return self.start(logger)

        def stop(self):
            if not self._started:
                return
            # 恢复被摘掉的 RichHandler，移除我们的 handler
            for lg, h in self._removed:
                lg.addHandler(h)
            for lg, _ in self._removed:
                for h in list(lg.handlers):
                    if isinstance(h, _PinnedLogHandler):
                        lg.removeHandler(h)
            self._live.stop()
            self._started = False

        def __enter__(self):
            if not self._started:
                self.start()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.stop()

        def add_task(self, description, total=100):
            return self.progress.add_task(description, total=total)

        def update(self, task_id, **kwargs):
            self.progress.update(task_id, **kwargs)

        def log(self, message, style=None):
            """向底部滚动日志区追加一行。style 形如 'red'/'yellow'/'dim'。
            对 1.5 秒内的完全重复消息进行去重，避免多 handler/多 logger 导致刷屏。"""
            msg = str(message)
            now = time.time()
            if msg == self._last_log_msg and (now - self._last_log_time) < self._log_dedup_window:
                return
            self._last_log_msg = msg
            self._last_log_time = now
            line = Text(msg)
            if style:
                line.stylize(style)
            self._buf.append(line)
            self._panel.renderable = Group(*self._buf)
            # 不在这里手动 refresh：Live 自带刷新循环（带 diff，只更新变化行）
            # 已能保证日志在 ~125ms 内显示出来，且不会与外部刷新线程交错写屏造成闪动。

else:
    # rich 不可用时的降级实现：纯打印，无进度条
    class PinnedProgress:
        def __init__(self, title='进度', max_log_lines=14, console=None, pin='bottom'):
            self._buf = deque(maxlen=max_log_lines)

        def start(self, logger=None):
            return self

        def bind(self, logger):
            return self

        def stop(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def add_task(self, description, total=100):
            return 0

        def update(self, task_id, **kwargs):
            pass

        def log(self, message, style=None):
            print(message)
