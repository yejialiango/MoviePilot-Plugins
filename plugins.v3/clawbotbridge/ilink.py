"""微信 ClawBot iLink 协议的最小客户端。

协议细节对照腾讯官方 OpenClaw 微信插件（@tencent-weixin/openclaw-weixin 2.4.9）：

* 扫码：``get_bot_qrcode`` 取二维码，``get_qrcode_status`` 长轮询状态；确认后返回
  ``bot_token``、``ilink_bot_id`` 以及扫码人的 ``ilink_user_id``，无需等对方先发消息；
* 收消息：``getupdates`` 长轮询，游标 ``get_updates_buf`` 需持久化；``errcode=-14`` 表示
  token 失效；
* 发消息：``sendmessage``，回复必须带对方最近一条消息的 ``context_token``。实测每个
  context_token 最多回复 10 条，同一 ``client_id`` 的后续更新不会在客户端刷新。

本模块只依赖 requests，不依赖 MoviePilot。
"""

from __future__ import annotations

import base64
import random
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

DEFAULT_BASE_URL = "https://ilinkai.weixin.qq.com"
BOT_TYPE = "3"
# 与官方插件保持一致：iLink-App-Id 取自其 package.json 的 ilink_appid，
# ClientVersion 由版本号 2.4.9 按 major<<16 | minor<<8 | patch 编码。
APP_ID = "bot"
CHANNEL_VERSION = "2.4.9"
CLIENT_VERSION = (2 << 16) | (4 << 8) | 9
BOT_AGENT = "MoviePilot-ClawBotBridge/1.0"

STALE_TOKEN_ERRCODE = -14

ITEM_TEXT = 1
ITEM_VOICE = 3
MSG_TYPE_USER = 1
MSG_TYPE_BOT = 2
MSG_STATE_FINISH = 2


class ILinkError(Exception):
    """iLink 返回非零 ret/errcode 或网络错误。"""

    def __init__(self, message: str, code: Optional[int] = None) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class InboundMessage:
    """一条来自微信用户的消息。"""

    message_id: str
    from_user_id: str
    text: str
    context_token: Optional[str]
    create_time_ms: int = 0
    raw: Dict[str, Any] = field(default_factory=dict)


def _uin() -> str:
    return base64.b64encode(str(random.getrandbits(32)).encode()).decode()


class ILinkClient:
    """面向单个 bot 账号的 iLink 调用封装。"""

    def __init__(self, token: Optional[str] = None, base_url: Optional[str] = None,
                 timeout: float = 15.0) -> None:
        self.token = token
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        # iLink 在国内，不走宿主给容器配的代理。
        self._session.trust_env = False

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass

    def _headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "AuthorizationType": "ilink_bot_token",
            "X-WECHAT-UIN": _uin(),
            "iLink-App-Id": APP_ID,
            "iLink-App-ClientVersion": str(CLIENT_VERSION),
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    @staticmethod
    def _base_info() -> Dict[str, str]:
        return {"channel_version": CHANNEL_VERSION, "bot_agent": BOT_AGENT}

    def _post(self, endpoint: str, body: Dict[str, Any], timeout: Optional[float] = None,
              base_url: Optional[str] = None) -> Dict[str, Any]:
        url = f"{(base_url or self.base_url).rstrip('/')}/{endpoint}"
        try:
            resp = self._session.post(url, json={**body, "base_info": self._base_info()},
                                      headers=self._headers(), timeout=timeout or self.timeout)
        except requests.RequestException as err:
            raise ILinkError(f"{endpoint} 请求失败：{err}") from err
        return self._parse(endpoint, resp)

    def _get(self, endpoint: str, params: Dict[str, Any], timeout: Optional[float] = None,
             base_url: Optional[str] = None) -> Dict[str, Any]:
        url = f"{(base_url or self.base_url).rstrip('/')}/{endpoint}"
        try:
            resp = self._session.get(url, params=params, headers=self._headers(),
                                     timeout=timeout or self.timeout)
        except requests.RequestException as err:
            raise ILinkError(f"{endpoint} 请求失败：{err}") from err
        return self._parse(endpoint, resp)

    @staticmethod
    def _parse(endpoint: str, resp: requests.Response) -> Dict[str, Any]:
        if resp.status_code != 200:
            raise ILinkError(f"{endpoint} HTTP {resp.status_code}：{resp.text[:200]}")
        try:
            data = resp.json()
        except ValueError as err:
            raise ILinkError(f"{endpoint} 返回非 JSON：{resp.text[:200]}") from err
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _check(endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        for key in ("ret", "errcode"):
            code = data.get(key)
            if code not in (None, 0):
                raise ILinkError(f"{endpoint} 失败：{key}={code} {data.get('errmsg') or ''}".strip(),
                                 code=int(code))
        return data

    # ---- 扫码登录 ----

    def get_qrcode(self) -> Dict[str, str]:
        """返回 ``{"qrcode": 轮询用的票据, "content": 需要编码成二维码的内容}``。"""
        data = self._check("get_bot_qrcode", self._post(
            f"ilink/bot/get_bot_qrcode?bot_type={BOT_TYPE}", {"local_token_list": []},
            base_url=DEFAULT_BASE_URL))
        qrcode = data.get("qrcode") or ""
        content = data.get("qrcode_img_content") or ""
        if not qrcode or not content:
            raise ILinkError(f"get_bot_qrcode 未返回二维码：{str(data)[:200]}")
        return {"qrcode": qrcode, "content": content}

    def get_qrcode_status(self, qrcode: str, base_url: Optional[str] = None,
                          verify_code: Optional[str] = None) -> Dict[str, Any]:
        """长轮询扫码状态；客户端超时视为 ``wait``。"""
        params = {"qrcode": qrcode}
        if verify_code:
            params["verify_code"] = verify_code
        try:
            return self._get("ilink/bot/get_qrcode_status", params, timeout=40,
                             base_url=base_url or DEFAULT_BASE_URL)
        except ILinkError as err:
            if "timed out" in str(err).lower() or "timeout" in str(err).lower():
                return {"status": "wait"}
            raise

    # ---- 收发消息 ----

    def get_updates(self, cursor: str, timeout: float = 40.0) -> Dict[str, Any]:
        """长轮询新消息，返回原始响应（含 msgs、get_updates_buf、longpolling_timeout_ms）。"""
        try:
            data = self._post("ilink/bot/getupdates", {"get_updates_buf": cursor or ""},
                              timeout=timeout)
        except ILinkError as err:
            if "timed out" in str(err).lower() or "timeout" in str(err).lower():
                return {"msgs": [], "get_updates_buf": cursor}
            raise
        return self._check("getupdates", data)

    @staticmethod
    def parse_inbound(msg: Dict[str, Any]) -> Optional[InboundMessage]:
        """只保留用户发来的文字（含语音转写）消息。"""
        if msg.get("message_type") not in (None, MSG_TYPE_USER):
            return None
        texts: List[str] = []
        for item in msg.get("item_list") or []:
            if item.get("type") == ITEM_TEXT and (item.get("text_item") or {}).get("text") is not None:
                texts.append(str(item["text_item"]["text"]))
            elif item.get("type") == ITEM_VOICE and (item.get("voice_item") or {}).get("text"):
                texts.append(str(item["voice_item"]["text"]))
        text = "\n".join(t for t in texts if t).strip()
        sender = msg.get("from_user_id") or ""
        if not text or not sender:
            return None
        return InboundMessage(
            message_id=str(msg.get("message_id") or msg.get("seq") or uuid.uuid4()),
            from_user_id=sender,
            text=text,
            context_token=msg.get("context_token"),
            create_time_ms=int(msg.get("create_time_ms") or 0),
            raw=msg,
        )

    def send_text(self, to_user_id: str, text: str, context_token: Optional[str],
                  run_id: Optional[str] = None) -> str:
        """发送一条完整的文字消息，返回服务端 message_id。"""
        msg: Dict[str, Any] = {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": f"cbb-{uuid.uuid4()}",
            "message_type": MSG_TYPE_BOT,
            "message_state": MSG_STATE_FINISH,
            "item_list": [{"type": ITEM_TEXT, "text_item": {"text": text}}],
        }
        if context_token:
            msg["context_token"] = context_token
        if run_id:
            msg["run_id"] = run_id
        data = self._check("sendmessage", self._post("ilink/bot/sendmessage", {"msg": msg}))
        return str(data.get("message_id") or "")
