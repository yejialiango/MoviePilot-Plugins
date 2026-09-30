"""微信 ClawBot 多账号桥接。

替代 MoviePilot 内置的「微信 ClawBot」通知渠道，解决三个问题：

* 多账号：一个插件里可以接入任意多个微信号，各自扫码；登录态存在插件数据里，不会像内置渠道
  那样放在 3 天过期的 Redis 缓存里、停机久了就丢；
* 少配置：扫码确认时 iLink 直接返回扫码人的 userid，账号只需在配置里选一个 MoviePilot 用户，
  Agent 就按该用户的权限运行，不再需要在用户管理、渠道白名单里分别填 userid；
* 分段流式：iLink 不支持原地更新消息，Agent 的回复按段落陆续发成多条气泡，受每条入站消息
  10 条回复的额度约束（见 segmenter.py）。

每个账号一个长轮询线程；配置保存只做差量调整，不重启正在运行的线程，避免内置渠道那种
「保存配置 → 重载时轮询线程收不回来 → 模块挂掉」的问题。
"""

from __future__ import annotations

import base64
import hashlib
import io
import threading
import time
import uuid
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Tuple

from app.db.oper.user import UserOper
from app.plugins import _PluginBase
from app.schemas.types import EventType, MessageType
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger

from .ilink import STALE_TOKEN_ERRCODE, ILinkClient, ILinkError, InboundMessage
from .segmenter import LineFilter, Segmenter, to_plain

# 默认推送的通知类型（MessageType 的中文值）。
DEFAULT_SWITCHES = [MessageType.Download.value, MessageType.Organize.value,
                    MessageType.Subscribe.value, MessageType.Manual.value]
SWITCH_ITEMS = [MessageType.Download, MessageType.Organize, MessageType.Subscribe,
                MessageType.SiteMessage, MessageType.MediaServer, MessageType.Manual,
                MessageType.Plugin, MessageType.Other]
NEW_SESSION_COMMANDS = {"/new", "/新对话", "新对话"}
STOP_COMMANDS = {"/stop", "/停止"}
LOGIN_TTL = 300


def _now() -> int:
    return int(time.time())


def _fmt_ts(ts: Optional[int]) -> str:
    return datetime.fromtimestamp(ts).strftime("%m-%d %H:%M") if ts else "—"


def _qr_data_url(content: str) -> Optional[str]:
    """把二维码内容渲染成 PNG data URL；缺少 qrcode 库时返回 None。"""
    try:
        import qrcode  # type: ignore
    except ImportError:
        return None
    img = qrcode.make(content, box_size=6, border=2)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()


class AccountWorker(threading.Thread):
    """单个 bot 账号的长轮询线程。"""

    def __init__(self, plugin: "ClawBotBridge", account_id: str) -> None:
        super().__init__(name=f"clawbotbridge-{account_id}", daemon=True)
        self.plugin = plugin
        self.account_id = account_id
        self.stop_event = threading.Event()
        self._seen: Deque[str] = deque(maxlen=300)

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        account = self.plugin.get_account(self.account_id)
        if not account:
            return
        client = ILinkClient(account["bot_token"], account.get("base_url"))
        timeout = 40.0
        failures = 0
        logger.info(f"[ClawBotBridge] 账号 {account['name']} 开始接收消息")
        try:
            while not self.stop_event.is_set():
                account = self.plugin.get_account(self.account_id)
                if not account or not account.get("enabled"):
                    break
                try:
                    resp = client.get_updates(account.get("cursor") or "", timeout=timeout)
                    failures = 0
                except ILinkError as err:
                    if err.code == STALE_TOKEN_ERRCODE:
                        logger.warning(f"[ClawBotBridge] 账号 {account['name']} 登录已失效，需要重新扫码")
                        self.plugin.update_account(self.account_id, status="expired")
                        break
                    failures += 1
                    logger.warning(f"[ClawBotBridge] 账号 {account['name']} 收消息失败（{failures}）：{err}")
                    self.stop_event.wait(30 if failures >= 3 else 3)
                    continue
                if self.stop_event.is_set():
                    # 已被要求停止：游标不推进，消息留给下一个线程处理。
                    break
                poll_ms = resp.get("longpolling_timeout_ms")
                if poll_ms:
                    timeout = max(10.0, poll_ms / 1000 + 5)
                cursor = resp.get("get_updates_buf")
                if cursor and cursor != account.get("cursor"):
                    self.plugin.update_account(self.account_id, cursor=cursor, status="online")
                for raw in resp.get("msgs") or []:
                    inbound = ILinkClient.parse_inbound(raw)
                    if not inbound or inbound.message_id in self._seen:
                        continue
                    self._seen.append(inbound.message_id)
                    self.plugin.on_inbound(self.account_id, inbound)
        finally:
            client.close()
            logger.info(f"[ClawBotBridge] 账号 {self.account_id} 停止接收消息")


class LoginSession(threading.Thread):
    """一次扫码登录：取二维码并长轮询状态，确认后写入账号。"""

    def __init__(self, plugin: "ClawBotBridge", relogin_id: Optional[str] = None) -> None:
        super().__init__(name="clawbotbridge-login", daemon=True)
        self.plugin = plugin
        self.relogin_id = relogin_id
        self.stop_event = threading.Event()
        self.started_at = _now()
        self.status = "init"
        self.message = "正在获取二维码…"
        self.qr_content: Optional[str] = None
        self.qr_image: Optional[str] = None
        self.verify_code: Optional[str] = None

    def run(self) -> None:
        client = ILinkClient()
        try:
            qr = client.get_qrcode()
            self.qr_content = qr["content"]
            self.qr_image = _qr_data_url(qr["content"])
            self.status, self.message = "wait", "请用要接入的微信扫码"
            base_url = None
            while not self.stop_event.is_set() and _now() - self.started_at < LOGIN_TTL:
                data = client.get_qrcode_status(qr["qrcode"], base_url=base_url,
                                                verify_code=self.verify_code)
                status = data.get("status") or "wait"
                self.status = status
                if status == "scaned":
                    self.message = "已扫码，请在手机上确认"
                elif status == "need_verifycode":
                    self.message = "请在插件配置的「配对数字」里填写手机上显示的数字并保存"
                    self.stop_event.wait(3)
                elif status == "verify_code_blocked":
                    self.message = "配对数字多次错误，已停止，请稍后重新添加"
                    return
                elif status == "scaned_but_redirect":
                    if data.get("redirect_host"):
                        base_url = f"https://{data['redirect_host']}"
                elif status == "binded_redirect":
                    self.message = "该微信已被其他客户端绑定（例如内置 ClawBot 渠道），请先在那边退出登录"
                    return
                elif status == "expired":
                    self.message = "二维码已过期，请重新添加"
                    return
                elif status == "confirmed":
                    self.plugin.on_login_confirmed(self.relogin_id, data)
                    self.message = "已连接"
                    return
            if not self.stop_event.is_set():
                self.status, self.message = "expired", "等待超时，请重新添加"
        except Exception as err:
            self.status, self.message = "error", f"登录失败：{err}"
            logger.error(f"[ClawBotBridge] 扫码登录失败：{err}")
        finally:
            client.close()


class ClawBotBridge(_PluginBase):
    """微信 ClawBot 多账号接入，Agent 分段流式回复与通知推送。"""

    plugin_name = "微信ClawBot多账号"
    plugin_desc = "多个微信号接入 MoviePilot 智能助手：扫码即绑定，按用户权限对话，回复分段陆续发出。"
    plugin_icon = "https://raw.githubusercontent.com/yejialiango/MoviePilot-Plugins/main/icons/Wechat_A.png"
    plugin_version = "0.1.0"
    plugin_label = "消息通知"
    plugin_author = "yejialiango"
    author_url = "https://github.com/yejialiango"
    plugin_config_prefix = "clawbotbridge_"
    plugin_order = 22
    auth_level = 1

    def __init__(self) -> None:
        super().__init__()
        self._enabled = False
        self._max_bubbles = 8
        self._min_chars = 60
        self._show_progress = True
        self._accounts: Dict[str, Dict[str, Any]] = {}
        self._workers: Dict[str, AccountWorker] = {}
        self._login: Optional[LoginSession] = None
        self._lock = threading.RLock()
        self._recent_notices: Dict[str, float] = {}

    # ---------------- 生命周期 ----------------

    def init_plugin(self, config: Optional[dict] = None) -> None:
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._max_bubbles = min(9, max(2, int(config.get("max_bubbles") or 8)))
        self._min_chars = max(0, int(config.get("min_chars") or 60))
        self._show_progress = config.get("show_progress", True)
        with self._lock:
            self._accounts = {a["id"]: a for a in (self.get_data("accounts") or [])}
            for acc_id, acc in self._accounts.items():
                prefix = f"acc_{acc_id}_"
                if f"{prefix}name" in config:
                    acc["name"] = (config.get(f"{prefix}name") or acc["name"]).strip()
                    acc["mp_username"] = config.get(f"{prefix}user") or ""
                    acc["switches"] = config.get(f"{prefix}switches") or []
                    acc["enabled"] = bool(config.get(f"{prefix}enabled", True))
            self._persist()
        if self._login and config.get("verify_code"):
            self._login.verify_code = str(config["verify_code"]).strip()
        self._reconcile_workers()

    def get_state(self) -> bool:
        return self._enabled

    def stop_service(self) -> None:
        try:
            if self._login:
                self._login.stop_event.set()
            with self._lock:
                workers = list(self._workers.values())
                self._workers.clear()
            for worker in workers:
                worker.stop()
            for worker in workers:
                worker.join(timeout=1)
        except Exception as err:
            logger.error(f"[ClawBotBridge] 停止失败：{err}")

    def _reconcile_workers(self) -> None:
        """只做差量：该停的停、该起的起，正在跑的不动。"""
        with self._lock:
            wanted = {
                acc_id for acc_id, acc in self._accounts.items()
                if self._enabled and acc.get("enabled") and acc.get("bot_token")
                and acc.get("status") != "expired"
            }
            for acc_id in list(self._workers):
                worker = self._workers[acc_id]
                if acc_id not in wanted or not worker.is_alive():
                    worker.stop()
                    del self._workers[acc_id]
            for acc_id in wanted - set(self._workers):
                worker = AccountWorker(self, acc_id)
                self._workers[acc_id] = worker
                worker.start()

    # ---------------- 账号存储 ----------------

    def _persist(self) -> None:
        self.save_data("accounts", list(self._accounts.values()))

    def get_account(self, account_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            acc = self._accounts.get(account_id)
            return dict(acc) if acc else None

    def update_account(self, account_id: str, **fields: Any) -> None:
        with self._lock:
            acc = self._accounts.get(account_id)
            if not acc:
                return
            acc.update(fields)
            self._persist()

    def _remember_peer(self, account_id: str, user_id: str, context_token: Optional[str]) -> None:
        with self._lock:
            acc = self._accounts.get(account_id)
            if not acc:
                return
            peers = acc.setdefault("peers", {})
            peer = peers.setdefault(user_id, {"epoch": 0})
            peer["last_active"] = _now()
            if context_token:
                peer["context_token"] = context_token
            self._persist()

    def on_login_confirmed(self, relogin_id: Optional[str], data: Dict[str, Any]) -> None:
        owner = data.get("ilink_user_id") or ""
        fields = {
            "bot_token": data.get("bot_token"),
            "bot_id": data.get("ilink_bot_id"),
            "base_url": data.get("baseurl") or None,
            "owner_userid": owner,
            "status": "online",
            "cursor": "",
            "logged_in_at": _now(),
        }
        with self._lock:
            if relogin_id and relogin_id in self._accounts:
                self._accounts[relogin_id].update(fields)
                acc_id = relogin_id
            else:
                acc_id = uuid.uuid4().hex[:8]
                self._accounts[acc_id] = {
                    "id": acc_id,
                    "name": f"微信{len(self._accounts) + 1}",
                    "mp_username": self._guess_mp_user(owner),
                    "switches": list(DEFAULT_SWITCHES),
                    "enabled": True,
                    "peers": {},
                    **fields,
                }
            self._persist()
        logger.info(f"[ClawBotBridge] 账号 {self._accounts[acc_id]['name']} 扫码登录成功")
        self._reconcile_workers()

    @staticmethod
    def _guess_mp_user(owner_userid: str) -> str:
        """沿用内置渠道时代在用户设置里填过的 wechatclawbot_userid。"""
        if not owner_userid:
            return ""
        for user in UserOper().list() or []:
            settings = user.settings or {}
            if isinstance(settings, dict) and settings.get("wechatclawbot_userid") == owner_userid:
                return user.name
        return ""

    # ---------------- 收消息 → Agent ----------------

    def on_inbound(self, account_id: str, inbound: InboundMessage) -> None:
        self._remember_peer(account_id, inbound.from_user_id, inbound.context_token)
        threading.Thread(target=self._handle_turn, args=(account_id, inbound),
                         name=f"clawbotbridge-turn-{account_id}", daemon=True).start()

    def _handle_turn(self, account_id: str, inbound: InboundMessage) -> None:
        # 延迟导入：宿主 Agent 模块只在运行时可用，也避免插件加载时拉起整套 Agent 依赖。
        from . import agent_bridge

        account = self.get_account(account_id)
        if not account:
            return
        client = ILinkClient(account["bot_token"], account.get("base_url"))
        run_id = uuid.uuid4().hex

        def send(text: str) -> bool:
            try:
                client.send_text(inbound.from_user_id, text, inbound.context_token, run_id)
                return True
            except ILinkError as err:
                logger.warning(f"[ClawBotBridge] 回复失败：{err}")
                return False

        try:
            username = account.get("mp_username")
            if not username or not agent_bridge.find_user(username):
                send("这个微信还没有绑定 MoviePilot 用户，请管理员在插件「微信ClawBot多账号」的配置里选择对应用户。")
                return
            peer = (account.get("peers") or {}).get(inbound.from_user_id, {"epoch": 0})
            user = agent_bridge.find_user(username)
            session_id = agent_bridge.make_session_id(user.id, inbound.from_user_id,
                                                      int(peer.get("epoch") or 0))
            command = inbound.text.strip().lower()
            if command in NEW_SESSION_COMMANDS:
                try:
                    agent_bridge.clear_session(user.id, session_id)
                except Exception as err:
                    logger.debug(f"[ClawBotBridge] 清理旧会话失败：{err}")
                with self._lock:
                    acc = self._accounts.get(account_id)
                    if acc:
                        p = acc.setdefault("peers", {}).setdefault(inbound.from_user_id, {})
                        p["epoch"] = int(p.get("epoch") or 0) + 1
                        self._persist()
                send("已开启新对话。")
                return
            if command in STOP_COMMANDS:
                stopped = agent_bridge.stop_turn(session_id)
                send("已停止当前任务。" if stopped else "当前没有正在进行的任务。")
                return

            seg = Segmenter(send, budget=self._max_bubbles, min_chars=self._min_chars)

            def on_summary(line: str) -> None:
                if self._show_progress:
                    seg.progress(f"⏳ {line.strip('（）')}")

            lines = LineFilter(seg.feed, on_summary)

            def on_tool(event: Dict[str, Any]) -> None:
                if self._show_progress and event.get("status") == "running":
                    seg.progress(f"⏳ {event.get('message') or event.get('tool_name') or '处理中'}")

            def on_message(message: Any) -> None:
                body = "\n".join(x for x in (getattr(message, "title", None),
                                            getattr(message, "text", None)) if x)
                if body:
                    lines.feed(f"\n\n{body}\n\n")

            try:
                notice = agent_bridge.run_turn(username, session_id, inbound.text,
                                               on_text=lines.feed, on_tool=on_tool,
                                               on_message=on_message)
                lines.flush()
                seg.finish(fallback=notice or "（没有回复内容）")
            except Exception as err:
                lines.flush()
                logger.error(f"[ClawBotBridge] 处理消息失败：{err}")
                seg.finish(fallback=f"⚠️ {err}")
                if seg.sent == 0:
                    send(f"⚠️ {err}")
        finally:
            client.close()

    # ---------------- 通知推送 ----------------

    @eventmanager.register(EventType.NoticeMessage)
    def on_notice(self, event: Event) -> None:
        if not self._enabled or not event or not event.event_data:
            return
        data = event.event_data
        mtype = data.get("type") or data.get("mtype")
        type_value = getattr(mtype, "value", mtype)
        # 定向回复到某个渠道的消息、以及智能体消息，不在这里重复推送。
        if data.get("channel") or type_value == MessageType.Agent.value:
            return
        title, text, link = data.get("title") or "", data.get("text") or "", data.get("link") or ""
        body = to_plain("\n".join(x for x in (title, text) if x))
        if link:
            body = f"{body}\n{link}"
        if not body:
            return
        digest = hashlib.md5(f"{type_value}|{body}".encode()).hexdigest()
        now = time.monotonic()
        with self._lock:
            self._recent_notices = {k: v for k, v in self._recent_notices.items() if now - v < 120}
            if digest in self._recent_notices:
                return
            self._recent_notices[digest] = now
            targets = [dict(a) for a in self._accounts.values()
                       if a.get("enabled") and a.get("bot_token") and a.get("status") != "expired"
                       and type_value in (a.get("switches") or [])]
        for acc in targets:
            owner = acc.get("owner_userid")
            peer = (acc.get("peers") or {}).get(owner) or {}
            if not owner or not peer.get("context_token"):
                logger.info(f"[ClawBotBridge] 账号 {acc['name']} 还没有会话凭证，跳过推送：{title}")
                continue
            client = ILinkClient(acc["bot_token"], acc.get("base_url"))
            try:
                client.send_text(owner, body, peer["context_token"])
            except ILinkError as err:
                logger.warning(f"[ClawBotBridge] 账号 {acc['name']} 推送失败（可能需要在微信里给 ClawBot 发一句话刷新额度）：{err}")
            finally:
                client.close()

    # ---------------- 插件 API ----------------

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/login/start", "endpoint": self.api_login_start, "methods": ["GET"],
             "auth": "bear", "summary": "开始扫码添加/重新登录账号"},
            {"path": "/login/cancel", "endpoint": self.api_login_cancel, "methods": ["GET"],
             "auth": "bear", "summary": "取消扫码"},
            {"path": "/account/remove", "endpoint": self.api_account_remove, "methods": ["GET"],
             "auth": "bear", "summary": "删除账号"},
            {"path": "/refresh", "endpoint": self.api_refresh, "methods": ["GET"],
             "auth": "bear", "summary": "刷新详情页"},
        ]

    @staticmethod
    def api_refresh() -> Dict[str, Any]:
        # 页面按钮触发后宿主会重新拉取 get_page，这里无需做事。
        return {"success": True}

    def api_login_start(self, account_id: str = "") -> Dict[str, Any]:
        if self._login and self._login.is_alive():
            self._login.stop_event.set()
        self._login = LoginSession(self, relogin_id=account_id or None)
        self._login.start()
        # 等二维码取回来，页面刷新时就能直接显示。
        for _ in range(20):
            if self._login.qr_content or self._login.status == "error":
                break
            time.sleep(0.25)
        return {"success": self._login.status != "error", "message": self._login.message}

    def api_login_cancel(self) -> Dict[str, Any]:
        if self._login:
            self._login.stop_event.set()
            self._login = None
        return {"success": True}

    def api_account_remove(self, account_id: str = "") -> Dict[str, Any]:
        with self._lock:
            acc = self._accounts.pop(account_id, None)
            worker = self._workers.pop(account_id, None)
            self._persist()
        if worker:
            worker.stop()
        return {"success": bool(acc), "message": "已删除" if acc else "账号不存在"}

    # ---------------- 页面 ----------------

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        users = [{"title": u.name + ("（管理员）" if u.is_superuser else ""), "value": u.name}
                 for u in (UserOper().list() or []) if u.is_active]
        switch_items = [{"title": t.value, "value": t.value} for t in SWITCH_ITEMS]
        model: Dict[str, Any] = {"enabled": False, "max_bubbles": 8, "min_chars": 60,
                                 "show_progress": True, "verify_code": ""}
        rows: List[dict] = [
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VSwitch", "props": {"model": "show_progress", "label": "发送工具进度"}}]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VTextField", "props": {"model": "verify_code", "label": "配对数字",
                                                          "hint": "扫码时手机要求输入数字才需要填", "persistent-hint": True}}]},
            ]},
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VTextField", "props": {"model": "max_bubbles", "label": "每轮最多气泡数",
                                                          "type": "number", "hint": "2-9，iLink 每条消息最多回复 10 条",
                                                          "persistent-hint": True}}]},
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VTextField", "props": {"model": "min_chars", "label": "单条最少字数",
                                                          "type": "number", "hint": "太短的段落会和下一段合并发送",
                                                          "persistent-hint": True}}]},
            ]},
        ]
        with self._lock:
            accounts = [dict(a) for a in self._accounts.values()]
        if not accounts:
            rows.append({"component": "VAlert", "props": {
                "type": "info", "variant": "tonal", "class": "mt-3",
                "text": "还没有接入微信。保存并启用插件后，到插件详情页点「添加微信」扫码。"}})
        for acc in accounts:
            prefix = f"acc_{acc['id']}_"
            model.update({f"{prefix}name": acc.get("name"), f"{prefix}user": acc.get("mp_username") or None,
                          f"{prefix}switches": acc.get("switches") or [],
                          f"{prefix}enabled": acc.get("enabled", True)})
            rows.append({"component": "VCard", "props": {"variant": "outlined", "class": "mt-3"}, "content": [
                {"component": "VCardTitle", "text": f"{acc.get('name')}（{acc.get('owner_userid') or '未知微信'}）"},
                {"component": "VCardText", "content": [{"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                        {"component": "VTextField", "props": {"model": f"{prefix}name", "label": "名称"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                        {"component": "VSelect", "props": {"model": f"{prefix}user", "label": "MoviePilot 用户",
                                                           "items": users, "clearable": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSelect", "props": {"model": f"{prefix}switches", "label": "推送通知",
                                                           "items": switch_items, "multiple": True, "chips": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VSwitch", "props": {"model": f"{prefix}enabled", "label": "启用"}}]},
                ]}]},
            ]})
        return [{"component": "VForm", "content": rows}], model

    def get_page(self) -> List[dict]:
        refresh = {"component": "VBtn", "props": {"text": "刷新", "variant": "tonal", "class": "mr-2"},
                   "events": {"click": {"api": "plugin/ClawBotBridge/refresh", "method": "get", "params": {}}}}
        page: List[dict] = []
        login = self._login
        if login and (login.is_alive() or login.status in ("confirmed", "error", "expired")):
            content: List[dict] = [{"component": "div", "props": {"class": "text-h6 mb-2"},
                                    "text": f"扫码状态：{login.message}"}]
            if login.qr_image and login.is_alive():
                content.append({"component": "VImg", "props": {"src": login.qr_image, "max-width": 260,
                                                               "class": "my-2"}})
            elif login.qr_content and login.is_alive():
                content.append({"component": "div", "text": f"二维码内容：{login.qr_content}"})
            content.append({"component": "div", "content": [
                {"component": "VBtn", "props": {"text": "刷新状态", "color": "primary", "class": "mr-2"},
                 "events": {"click": {"api": "plugin/ClawBotBridge/refresh", "method": "get", "params": {}}}},
                {"component": "VBtn", "props": {"text": "关闭", "variant": "tonal"},
                 "events": {"click": {"api": "plugin/ClawBotBridge/login/cancel", "method": "get", "params": {}}}},
            ]})
            page.append({"component": "VCard", "props": {"variant": "outlined", "class": "mb-4 pa-4"},
                         "content": content})
        with self._lock:
            accounts = [dict(a) for a in self._accounts.values()]
            alive = {k for k, w in self._workers.items() if w.is_alive()}
        status_text = {"online": "在线", "expired": "登录失效"}
        for acc in accounts:
            state = "接收中" if acc["id"] in alive else status_text.get(acc.get("status"), "未运行")
            owner = acc.get("owner_userid")
            peer = (acc.get("peers") or {}).get(owner) or {}
            page.append({"component": "VCard", "props": {"variant": "outlined", "class": "mb-3 pa-3"}, "content": [
                {"component": "div", "props": {"class": "text-subtitle-1"},
                 "text": f"{acc.get('name')} · {state} · 绑定用户：{acc.get('mp_username') or '未绑定'}"},
                {"component": "div", "props": {"class": "text-caption"},
                 "text": f"微信 userid：{owner or '—'} · 最近互动：{_fmt_ts(peer.get('last_active'))}"
                         f" · 登录时间：{_fmt_ts(acc.get('logged_in_at'))}"},
                {"component": "div", "props": {"class": "mt-2"}, "content": [
                    {"component": "VBtn", "props": {"text": "重新扫码", "size": "small", "variant": "tonal",
                                                    "class": "mr-2"},
                     "events": {"click": {"api": "plugin/ClawBotBridge/login/start", "method": "get",
                                          "params": {"account_id": acc["id"]}}}},
                    {"component": "VBtn", "props": {"text": "删除", "size": "small", "color": "error",
                                                    "variant": "tonal"},
                     "events": {"click": {"api": "plugin/ClawBotBridge/account/remove", "method": "get",
                                          "params": {"account_id": acc["id"]}}}},
                ]},
            ]})
        if not accounts and not page:
            page.append({"component": "VAlert", "props": {"type": "info", "variant": "tonal", "class": "mb-3",
                                                          "text": "还没有接入微信，点下面的「添加微信」扫码。"}})
        page.append({"component": "div", "content": [
            {"component": "VBtn", "props": {"text": "添加微信", "color": "primary", "class": "mr-2"},
             "events": {"click": {"api": "plugin/ClawBotBridge/login/start", "method": "get", "params": {}}}},
            refresh,
        ]})
        return page
