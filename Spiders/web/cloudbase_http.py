#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CloudBase（微信云开发）HTTP API 客户端（标准库实现）

参考：
  - https://developers.weixin.qq.com/miniprogram/dev/wxcloudservice/wxcloud/reference-http-api/

本模块不会把 AppSecret 写入代码仓库；请用环境变量或 .env 文件注入：
  - WECHAT_APPID
  - WECHAT_APPSECRET
  - CLOUDBASE_ENV_ID
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


WECHAT_TOKEN_URL = "https://api.weixin.qq.com/cgi-bin/token"
TcbApiBase = "https://api.weixin.qq.com/tcb/"


class CloudBaseError(RuntimeError):
    pass


def _now_ts() -> int:
    return int(time.time())


def load_dotenv(dotenv_path: str) -> None:
    """
    Very small .env loader (KEY=VALUE per line). Existing os.environ wins.
    """
    if not dotenv_path or not os.path.exists(dotenv_path):
        return
    with open(dotenv_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def _http_json(
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    *,
    timeout_sec: int = 20,
) -> Dict[str, Any]:
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST" if data else "GET")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    try:
        return json.loads(body)
    except Exception as e:
        raise CloudBaseError(f"HTTP response is not JSON. url={url}, body_prefix={body[:200]!r}") from e


@dataclass(frozen=True)
class CloudBaseConfig:
    env_id: str
    appid: str
    appsecret: str
    cache_dir: str


def get_cloudbase_config(
    *,
    dotenv_path: Optional[str] = None,
    cache_dir: Optional[str] = None,
) -> CloudBaseConfig:
    if dotenv_path:
        load_dotenv(dotenv_path)

    env_id = os.getenv("CLOUDBASE_ENV_ID", "").strip()
    appid = os.getenv("WECHAT_APPID", "").strip()
    appsecret = os.getenv("WECHAT_APPSECRET", "").strip()
    if not env_id or not appid or not appsecret:
        raise CloudBaseError(
            "Missing env config. Require CLOUDBASE_ENV_ID, WECHAT_APPID, WECHAT_APPSECRET "
            f"(got env_id={bool(env_id)}, appid={bool(appid)}, appsecret={bool(appsecret)})."
        )

    if cache_dir is None:
        # Default: <this_dir>/.cache
        cache_dir = os.path.join(os.path.dirname(__file__), ".cache")
    os.makedirs(cache_dir, exist_ok=True)

    return CloudBaseConfig(env_id=env_id, appid=appid, appsecret=appsecret, cache_dir=cache_dir)


class CloudBaseClient:
    def __init__(self, cfg: CloudBaseConfig):
        self.cfg = cfg
        self._token_cache_path = os.path.join(cfg.cache_dir, "wechat_access_token.json")

    def _read_cached_token(self) -> Optional[Tuple[str, int]]:
        if not os.path.exists(self._token_cache_path):
            return None
        try:
            with open(self._token_cache_path, "r", encoding="utf-8") as f:
                d = json.load(f)
            token = d.get("access_token")
            expires_at = int(d.get("expires_at", 0))
            if token and expires_at > _now_ts() + 30:
                return token, expires_at
        except Exception:
            return None
        return None

    def _write_cached_token(self, token: str, expires_in: int) -> None:
        # Refresh 60s earlier to be safe.
        expires_at = _now_ts() + max(0, int(expires_in) - 60)
        with open(self._token_cache_path, "w", encoding="utf-8") as f:
            json.dump({"access_token": token, "expires_at": expires_at}, f, ensure_ascii=False, indent=2)

    def get_access_token(self, *, force_refresh: bool = False) -> str:
        if not force_refresh:
            cached = self._read_cached_token()
            if cached:
                return cached[0]

        params = {
            "grant_type": "client_credential",
            "appid": self.cfg.appid,
            "secret": self.cfg.appsecret,
        }
        url = WECHAT_TOKEN_URL + "?" + urllib.parse.urlencode(params)
        resp = _http_json(url, None)
        if "access_token" not in resp:
            raise CloudBaseError(f"Failed to get access_token: {resp}")
        token = resp["access_token"]
        expires_in = int(resp.get("expires_in", 7200))
        self._write_cached_token(token, expires_in)
        return token

    def _call_tcb(self, endpoint: str, payload: Dict[str, Any], *, retry: int = 2) -> Dict[str, Any]:
        token = self.get_access_token()
        url = f"{TcbApiBase}{endpoint}?access_token={urllib.parse.quote(token)}"
        last_err = None
        for i in range(retry + 1):
            try:
                resp = _http_json(url, payload)
                # WeChat-style error fields
                errcode = int(resp.get("errcode", 0) or 0)
                if errcode != 0:
                    raise CloudBaseError(f"TCB API error. endpoint={endpoint}, resp={resp}")
                return resp
            except Exception as e:
                last_err = e
                # token may expire; refresh once
                if i == 0:
                    try:
                        self.get_access_token(force_refresh=True)
                    except Exception:
                        pass
                time.sleep(0.6 * (2 ** i))
        raise CloudBaseError(f"TCB API call failed after retries. endpoint={endpoint}") from last_err

    # ---- Cloud Functions ----
    def call_function(self, *, name: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        调用云函数（https://api.weixin.qq.com/tcb/invokecloudfunction）
        """
        payload = {
            "env": self.cfg.env_id,
            "name": name,
        }
        if data is not None:
            payload["data"] = json.dumps(data, ensure_ascii=False)
        return self._call_tcb("invokecloudfunction", payload)

    def database_query(self, query: str) -> Dict[str, Any]:
        return self._call_tcb("databasequery", {"env": self.cfg.env_id, "query": query})

    def database_update(self, query: str) -> Dict[str, Any]:
        return self._call_tcb("databaseupdate", {"env": self.cfg.env_id, "query": query})

    def database_delete(self, query: str) -> Dict[str, Any]:
        return self._call_tcb("databasedelete", {"env": self.cfg.env_id, "query": query})

    # Convenience wrappers
    def doc_set(self, *, collection: str, doc_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        js_obj = json.dumps(data, ensure_ascii=False)
        query = f'db.collection("{collection}").doc("{doc_id}").set({{data: {js_obj}}})'
        return self.database_update(query)

    def doc_get(self, *, collection: str, doc_id: str) -> Dict[str, Any]:
        query = f'db.collection("{collection}").doc("{doc_id}").get()'
        return self.database_query(query)

    def where_get(
        self,
        *,
        collection: str,
        where_obj: Dict[str, Any],
        order_by: Optional[Tuple[str, str]] = None,
        limit: int = 100,
        skip: int = 0,
    ) -> Dict[str, Any]:
        where_js = json.dumps(where_obj, ensure_ascii=False)
        q = f'db.collection("{collection}").where({where_js})'
        if order_by:
            field, direction = order_by
            q += f'.orderBy("{field}", "{direction}")'
        q += f".skip({int(skip)}).limit({int(limit)}).get()"
        return self.database_query(q)

    def where_count(self, *, collection: str, where_obj: Dict[str, Any]) -> Dict[str, Any]:
        where_js = json.dumps(where_obj, ensure_ascii=False)
        q = f'db.collection("{collection}").where({where_js}).count()'
        return self.database_query(q)

