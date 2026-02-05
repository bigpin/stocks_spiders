#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工具开关管理脚本

用于管理云数据库中 tools_switch 集合的开关状态。
主要用于小程序提交审核时临时禁用某些工具，审核通过后再启用。

使用方法：
    # 查看所有开关状态
    python manage_tool_switch.py --list
    
    # 禁用工具（提交审核前）
    python manage_tool_switch.py --disable stock-signals
    
    # 启用工具（审核通过后）
    python manage_tool_switch.py --enable stock-signals
    
    # 初始化开关记录（首次使用）
    python manage_tool_switch.py --init stock-signals

数据库结构：
    集合名：tools_switch
    文档格式：
    {
        "_id": "switch_stock-signals",
        "tool_id": "stock-signals",
        "enabled": true,
        "updated_at": "2026-02-04T12:00:00+0800"
    }
"""

import argparse
import json
import os
import sys
import time
from typing import List, Optional

# 添加父目录到路径，以便导入 cloudbase_lib
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from cloudbase_lib import CloudBaseClient, get_cloudbase_config, CloudBaseError

# 集合名称
COLLECTION_NAME = "tools_switch"

# 需要开关控制的工具ID列表
SWITCH_CONTROLLED_TOOLS = [
    "stock-signals",
]


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def make_switch_id(tool_id: str) -> str:
    """生成开关记录的 _id"""
    return f"switch_{tool_id}"


def get_client(dotenv: str = "") -> CloudBaseClient:
    """获取云数据库客户端"""
    if not dotenv:
        # 尝试多个可能的 .env 位置
        possible_paths = [
            os.path.join(os.path.dirname(__file__), ".env"),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "cloudbase_lib", ".env"),
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "Spiders", "web", ".env"),
        ]
        for path in possible_paths:
            if os.path.exists(path):
                dotenv = path
                break
    
    cfg = get_cloudbase_config(dotenv_path=dotenv or None)
    return CloudBaseClient(cfg)


def _parse_db_data(data) -> List[dict]:
    """解析云数据库返回的数据"""
    if not data:
        return []
    
    # data 可能是字符串、列表等
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return []
    
    if not isinstance(data, list):
        data = [data]
    
    # 列表中的每个元素可能也是 JSON 字符串
    result = []
    for item in data:
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except json.JSONDecodeError:
                continue
        if isinstance(item, dict):
            result.append(item)
    
    return result


def list_switches(client: CloudBaseClient) -> List[dict]:
    """列出所有工具开关状态"""
    try:
        resp = client.where_get(
            collection=COLLECTION_NAME,
            where_obj={},
            limit=100
        )
        return _parse_db_data(resp.get("data", []))
    except CloudBaseError as e:
        if "not exist" in str(e).lower() or "-502005" in str(e):
            print(f"[WARN] 集合 {COLLECTION_NAME} 不存在，请先初始化")
            return []
        raise


def get_switch(client: CloudBaseClient, tool_id: str) -> Optional[dict]:
    """获取单个工具的开关状态"""
    switch_id = make_switch_id(tool_id)
    try:
        resp = client.doc_get(collection=COLLECTION_NAME, doc_id=switch_id)
        data = _parse_db_data(resp.get("data", []))
        if data and len(data) > 0:
            return data[0]
        return None
    except CloudBaseError:
        return None


def set_switch(client: CloudBaseClient, tool_id: str, enabled: bool) -> bool:
    """设置工具开关状态"""
    switch_id = make_switch_id(tool_id)
    data = {
        "tool_id": tool_id,
        "enabled": enabled,
        "updated_at": _now_iso(),
    }
    
    try:
        client.doc_set(collection=COLLECTION_NAME, doc_id=switch_id, data=data)
        return True
    except CloudBaseError as e:
        print(f"[ERROR] 设置开关失败: {e}")
        return False


def init_switch(client: CloudBaseClient, tool_id: str, enabled: bool = True) -> bool:
    """初始化工具开关记录"""
    existing = get_switch(client, tool_id)
    if existing:
        print(f"[INFO] 开关记录已存在: {tool_id} = {existing.get('enabled')}")
        return True
    
    return set_switch(client, tool_id, enabled)


def main():
    parser = argparse.ArgumentParser(
        description="工具开关管理脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 查看所有开关状态
    python manage_tool_switch.py --list
    
    # 禁用工具（提交审核前）
    python manage_tool_switch.py --disable stock-signals
    
    # 启用工具（审核通过后）
    python manage_tool_switch.py --enable stock-signals
    
    # 初始化开关记录
    python manage_tool_switch.py --init stock-signals
    
    # 初始化所有预定义工具的开关
    python manage_tool_switch.py --init-all
"""
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list", "-l", action="store_true", help="列出所有工具开关状态")
    group.add_argument("--enable", "-e", metavar="TOOL_ID", help="启用指定工具")
    group.add_argument("--disable", "-d", metavar="TOOL_ID", help="禁用指定工具")
    group.add_argument("--init", "-i", metavar="TOOL_ID", help="初始化指定工具的开关记录")
    group.add_argument("--init-all", action="store_true", help="初始化所有预定义工具的开关记录")
    
    parser.add_argument("--dotenv", default="", help=".env 文件路径")
    
    args = parser.parse_args()
    
    try:
        client = get_client(args.dotenv)
        print(f"[INFO] 已连接到云数据库")
    except Exception as e:
        print(f"[ERROR] 连接云数据库失败: {e}")
        return 1
    
    # 列出所有开关
    if args.list:
        switches = list_switches(client)
        if not switches:
            print(f"[INFO] 没有找到任何开关记录")
            print(f"[TIP] 使用 --init-all 初始化所有预定义工具的开关")
        else:
            print(f"\n{'工具ID':<20} {'状态':<10} {'更新时间'}")
            print("-" * 60)
            for sw in switches:
                tool_id = sw.get("tool_id", "unknown")
                enabled = sw.get("enabled", False)
                updated_at = sw.get("updated_at", "N/A")
                status = "✅ 启用" if enabled else "❌ 禁用"
                print(f"{tool_id:<20} {status:<10} {updated_at}")
        return 0
    
    # 启用工具
    if args.enable:
        tool_id = args.enable
        print(f"[INFO] 正在启用工具: {tool_id}")
        if set_switch(client, tool_id, True):
            print(f"[OK] 工具 {tool_id} 已启用 ✅")
            return 0
        else:
            return 1
    
    # 禁用工具
    if args.disable:
        tool_id = args.disable
        print(f"[INFO] 正在禁用工具: {tool_id}")
        if set_switch(client, tool_id, False):
            print(f"[OK] 工具 {tool_id} 已禁用 ❌")
            return 0
        else:
            return 1
    
    # 初始化单个工具
    if args.init:
        tool_id = args.init
        print(f"[INFO] 正在初始化工具开关: {tool_id}")
        if init_switch(client, tool_id, enabled=True):
            print(f"[OK] 工具 {tool_id} 开关已初始化（默认启用）")
            return 0
        else:
            return 1
    
    # 初始化所有预定义工具
    if args.init_all:
        print(f"[INFO] 正在初始化所有预定义工具的开关...")
        print(f"[INFO] 预定义工具列表: {SWITCH_CONTROLLED_TOOLS}")
        
        success_count = 0
        for tool_id in SWITCH_CONTROLLED_TOOLS:
            print(f"  - 初始化 {tool_id}...")
            if init_switch(client, tool_id, enabled=True):
                success_count += 1
        
        print(f"[OK] 初始化完成: {success_count}/{len(SWITCH_CONTROLLED_TOOLS)}")
        return 0 if success_count == len(SWITCH_CONTROLLED_TOOLS) else 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
