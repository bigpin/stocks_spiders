#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试云函数调用的脚本

用法:
    python test_cloud_function.py
    python test_cloud_function.py --function checkStockSignals --date 20260203
"""

import argparse
import os
import sys

# 添加父目录到路径，以便导入 cloudbase_lib
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import sys as _sys, os as _os
_p = _os.path.dirname(_os.path.abspath(__file__))
while _p and _p != _os.path.dirname(_p) and not _os.path.isdir(_os.path.join(_p, 'Spiders')):
    _p = _os.path.dirname(_p)
if _p and _os.path.isdir(_os.path.join(_p, 'Spiders')) and _p not in _sys.path:
    _sys.path.insert(0, _p)
from Spiders.common.log import get_logger
logger = get_logger(__name__)

from cloudbase_lib import CloudBaseClient, get_cloudbase_config


def _find_dotenv() -> str:
    """查找 .env 文件"""
    possible_paths = [
        os.path.join(os.path.dirname(__file__), ".env"),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "cloudbase_lib", ".env"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "Spiders", "web", ".env"),
    ]
    for path in possible_paths:
        if os.path.exists(path):
            return path
    return ""


def main():
    parser = argparse.ArgumentParser(description="测试云函数调用")
    parser.add_argument("--function", default="checkStockSignals", help="云函数名称")
    parser.add_argument("--action", default="test", choices=["test", "manual"], help="触发模式: test=测试, manual=正式触发")
    parser.add_argument("--date", default=None, help="报告日期 (YYYY-MM-DD 格式)")
    parser.add_argument("--dotenv", default="", help=".env 文件路径")
    args = parser.parse_args()

    # 加载配置
    dotenv = args.dotenv.strip() or _find_dotenv()

    logger.info(f"[INFO] 加载配置文件: {dotenv or '(使用环境变量)'}")

    try:
        cfg = get_cloudbase_config(dotenv_path=dotenv or None)
        logger.info(f"[INFO] 环境ID: {cfg.env_id}")
        logger.info(f"[INFO] AppID: {cfg.appid[:8]}...（已隐藏）")
    except Exception as e:
        logger.error(f"[ERROR] 配置加载失败: {e}")
        return 1

    client = CloudBaseClient(cfg)

    # 先测试 access_token 获取
    logger.info("\n[TEST 1] 测试获取 access_token...")
    try:
        token = client.get_access_token()
        logger.info(f"[OK] access_token 获取成功: {token[:20]}...")
    except Exception as e:
        logger.error(f"[ERROR] access_token 获取失败: {e}")
        return 1

    # 测试云函数调用
    logger.info(f"\n[TEST 2] 调用云函数 '{args.function}' (action={args.action})...")
    
    data = {"action": args.action}
    if args.date:
        data["reportDate"] = args.date
    
    logger.info(f"[INFO] 调用参数: {data}")
    
    try:
        resp = client.call_function(name=args.function, data=data)
        logger.info(f"[OK] 云函数调用成功!")
        logger.info(f"[INFO] 响应: {resp}")
        
        # 解析云函数返回值
        if "resp_data" in resp:
            logger.info(f"[INFO] 云函数返回数据: {resp['resp_data']}")
    except Exception as e:
        logger.error(f"[ERROR] 云函数调用失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
    # cd /Users/dingli/Documents/UGit/Spiders/Spiders/web && python test_cloud_function.py --action manual --date 2026-02-03
    