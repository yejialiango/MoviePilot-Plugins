"""以指定 MoviePilot 用户身份运行一轮 AI Agent，并把输出增量回调出来。

走的是 Web AI 对话页面同一条内部路径（``AgentSessionOwner.process_message`` + WebAgent 类型），
而不是 OpenAI 兼容接口：后者一律按管理员处理，这里需要权限跟随绑定用户。

* WebAgent 按数字 ``user_id`` 回查用户的 ``is_superuser`` 决定管理员身份，普通用户调用管理员工具
  会被拒绝，API 工具也以该用户身份执行（app/agent/web.py、app/agent/tools/impl/api.py）；
* ``output_callback`` 收到的是增量文本；``tool_event_callback`` 只在 AI_AGENT_VERBOSE 开启或
  下面的 ``_force_structured_tools`` 生效时触发，否则进度以「（执行了 N 次搜索）」行混在正文里；
* 回调运行在宿主主事件循环线程上，只能入队，不能阻塞。

依赖宿主内部接口，MoviePilot 大版本升级时需要复核。
"""

from __future__ import annotations

import asyncio
import hashlib
import queue
import time
from typing import Any, Callable, Dict, Optional

from app.application import agent as agent_app
from app.application.configuration import get_api_runtime_config_snapshot
from app.db.oper.user import UserOper
from app.runtime.loop import main_loop_registry
from app.schemas.types import NotificationChannel, ReplyMode
from app.sdk.logging import logger

SOURCE = "clawbotbridge"
SESSION_PREFIX = "clawbot:"


class AgentUnavailable(RuntimeError):
    """Agent 未启用、未运行或排队已满。"""


def make_session_id(mp_user_id: Any, wx_user_id: str, epoch: int) -> str:
    """每个（MP 用户, 微信用户, 会话代次）对应一个独立会话。"""
    digest = hashlib.sha256(f"{mp_user_id}:{wx_user_id}:{epoch}".encode()).hexdigest()[:32]
    return SESSION_PREFIX + digest


def _manager_and_loop():
    if not get_api_runtime_config_snapshot().ai_agent_enable:
        raise AgentUnavailable("MoviePilot 未开启 AI 智能体")
    manager = agent_app.get_running_agent_manager()
    if manager is None:
        raise AgentUnavailable("MoviePilot AI 智能体未运行")
    return manager, main_loop_registry.require()


def _force_structured_tools(agent: Any) -> None:
    """不依赖全局 AI_AGENT_VERBOSE 也拿到结构化工具事件（私有钩子，缺失时静默降级）。"""
    handler = getattr(agent, "stream_handler", None)
    if handler is not None and hasattr(handler, "_uses_structured_tool_events"):
        handler._uses_structured_tool_events = (
            lambda: getattr(handler, "_on_tool_event", None) is not None
        )


def find_user(username: str):
    """按用户名取启用中的 MoviePilot 用户。"""
    user = UserOper().get_by_name(username) if username else None
    if not user or not user.is_active:
        return None
    return user


def run_turn(username: str, session_id: str, text: str,
             on_text: Callable[[str], None], on_tool: Callable[[Dict[str, Any]], None],
             on_message: Optional[Callable[[Any], None]] = None,
             timeout: float = 600.0) -> Optional[str]:
    """阻塞执行一轮对话（在插件线程里调用）。

    返回宿主给出的错误/提示文本（成功时通常为 None，正文已通过 on_text 流出）。
    """
    manager, loop = _manager_and_loop()
    user = find_user(username)
    if user is None:
        raise PermissionError(f"MoviePilot 用户 {username!r} 不存在或已停用")

    events: "queue.Queue[tuple[str, Any]]" = queue.Queue()

    def output_cb(delta: str) -> None:
        events.put(("text", delta))

    def tool_cb(event: Dict[str, Any]) -> None:
        events.put(("tool", dict(event)))

    async def message_cb(message: Any) -> None:
        events.put(("message", message))

    coro = manager.process_message(
        session_id=session_id,
        user_id=str(user.id),
        message=text,
        channel=NotificationChannel.WebAgent.value,
        source=SOURCE,
        username=user.name,
        reply_mode=ReplyMode.CAPTURE_ONLY,
        allow_message_tools=True,
        output_callback=output_cb,
        tool_event_callback=tool_cb,
        message_callback=message_cb,
        agent_factory=agent_app.get_web_agent_type(),
        agent_setup=_force_structured_tools,
        wait_for_completion=True,
    )
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    future.add_done_callback(lambda _f: events.put(("done", None)))

    deadline = time.monotonic() + timeout
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("智能助手处理超时")
            kind, payload = events.get(timeout=remaining)
            if kind == "text":
                on_text(payload)
            elif kind == "tool":
                on_tool(payload)
            elif kind == "message" and on_message:
                on_message(payload)
            elif kind == "done":
                break
        try:
            result = future.result()
        except asyncio.CancelledError as err:
            raise AgentUnavailable("本轮对话已被停止") from err
        except Exception as err:
            code = getattr(err, "code", None)
            if code == "agent_manager_queue_full":
                raise AgentUnavailable("智能助手当前排队已满，请稍后再试") from err
            if code == "agent_manager_unavailable":
                raise AgentUnavailable("智能助手服务暂不可用") from err
            raise
        return str(result).strip() if result else None
    finally:
        if not future.done():
            # 取消等待不会停止宿主里的 worker，必须显式停。
            try:
                asyncio.run_coroutine_threadsafe(
                    manager.stop_current_task(session_id), loop).result(15)
            except Exception as err:
                logger.debug(f"[ClawBotBridge] 停止超时会话失败：{err}")
            future.cancel()


def stop_turn(session_id: str) -> bool:
    manager, loop = _manager_and_loop()
    return bool(asyncio.run_coroutine_threadsafe(
        manager.stop_current_task(session_id), loop).result(15))


def clear_session(mp_user_id: Any, session_id: str) -> None:
    """清掉内存中的会话；调用方需同时递增代次，确保下一轮换新 session_id。"""
    manager, loop = _manager_and_loop()
    asyncio.run_coroutine_threadsafe(
        manager.clear_session(session_id=session_id, user_id=str(mp_user_id)), loop).result(30)
