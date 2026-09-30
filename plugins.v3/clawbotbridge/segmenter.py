"""把 Agent 的流式输出切成若干条微信气泡。

iLink 不支持原地更新消息（同一 client_id 只显示第一段），而且每条入站消息最多能回复
10 条，所以「流式」只能做成「按段落陆续发出多条气泡」，并且必须在额度内收尾：

* 攒够一个完整段落（遇到空行）且长度达到 ``min_chars`` 才发，避免一两个字一条；
* 段落太短时继续攒，超过 ``max_wait`` 秒没发过也会先发出去，保证有「在动」的体感；
* 始终为最终收尾预留 1 条额度：额度只剩 1 条时停止中途发送，剩余内容在 ``finish`` 时合并发出；
* 单条超过 ``max_chars`` 时按行切开，但切分数不会突破剩余额度，放不下的合并进最后一条。

本模块不依赖 MoviePilot，便于单测。
"""

from __future__ import annotations

import re
import time
from typing import Callable, List, Optional

# 非 verbose 模式下，Agent 把工具进度写成独立一行的「（执行了 2 次搜索）」混在正文里。
_SUMMARY_LINE = re.compile(r"^\s*（[^（）\n]{1,80}）\s*$")
_CODE_FENCE = re.compile(r"^\s*```[\w-]*\s*$")
_HEADING = re.compile(r"^\s{0,3}#{1,6}\s+")
_BOLD = re.compile(r"\*\*(.+?)\*\*|__(.+?)__")
_LINK = re.compile(r"\[([^\]]+)\]\((https?://[^)\s]+)\)")


def is_summary_line(line: str) -> bool:
    """判断是否为 Agent 在正文里插入的工具进度摘要行。"""
    return bool(_SUMMARY_LINE.match(line))


def to_plain(text: str) -> str:
    """把常见 Markdown 收敛成微信里能直接读的纯文本。"""
    lines = []
    for line in text.splitlines():
        if _CODE_FENCE.match(line):
            continue
        line = _HEADING.sub("", line)
        line = _BOLD.sub(lambda m: m.group(1) or m.group(2), line)
        line = _LINK.sub(lambda m: f"{m.group(1)} {m.group(2)}", line)
        lines.append(line.rstrip())
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def split_long(text: str, max_chars: int, max_parts: int) -> List[str]:
    """按行把超长文本切成不超过 ``max_parts`` 段；放不下的并入最后一段。"""
    if max_parts <= 1 or len(text) <= max_chars:
        return [text]
    parts: List[str] = []
    current = ""
    for line in text.split("\n"):
        candidate = f"{current}\n{line}" if current else line
        if current and len(candidate) > max_chars and len(parts) < max_parts - 1:
            parts.append(current)
            current = line
        else:
            current = candidate
    if current:
        parts.append(current)
    return parts


class LineFilter:
    """把增量文本按行拆开，识别出进度摘要行交给 ``on_summary``，其余透传给 ``on_text``。"""

    def __init__(self, on_text: Callable[[str], None], on_summary: Callable[[str], None]) -> None:
        self._on_text = on_text
        self._on_summary = on_summary
        self._partial = ""

    def feed(self, delta: str) -> None:
        if not delta:
            return
        self._partial += delta
        *lines, self._partial = self._partial.split("\n")
        for line in lines:
            if is_summary_line(line):
                self._on_summary(line.strip())
            else:
                self._on_text(line + "\n")

    def flush(self) -> None:
        if self._partial:
            line, self._partial = self._partial, ""
            if is_summary_line(line):
                self._on_summary(line.strip())
            else:
                self._on_text(line)


class Segmenter:
    """把增量文本切成气泡并通过 ``send`` 发出，严格遵守单轮额度。"""

    def __init__(
        self,
        send: Callable[[str], bool],
        budget: int = 8,
        min_chars: int = 60,
        max_chars: int = 1800,
        max_wait: float = 8.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._send = send
        self._budget = max(1, budget)
        self._min_chars = min_chars
        self._max_chars = max_chars
        self._max_wait = max_wait
        self._clock = clock
        self._pending = ""
        self._sent = 0
        self._progress_sent = 0
        self._last_sent_at = clock()
        self._any_text = False

    @property
    def remaining(self) -> int:
        return self._budget - self._sent

    @property
    def sent(self) -> int:
        return self._sent

    def _emit(self, text: str) -> None:
        text = text.strip()
        if not text or self.remaining <= 0:
            return
        if self._send(text):
            self._sent += 1
            self._last_sent_at = self._clock()

    def progress(self, text: str, max_progress_share: int = 2) -> bool:
        """发送一条进度气泡；本轮最多 ``max_progress_share`` 条，且至少给正文留 2 条额度。"""
        if self.remaining <= 2 or self._progress_sent >= max_progress_share:
            return False
        before = self._sent
        self._emit(text)
        if self._sent > before:
            self._progress_sent += 1
            return True
        return False

    def feed(self, delta: str) -> None:
        """接收一段增量文本，凑够完整段落就发出。"""
        if not delta:
            return
        self._pending += delta
        self._any_text = True
        # 只剩收尾额度时不再中途发送。
        while self.remaining > 1:
            cut = self._pending.find("\n\n")
            if cut < 0:
                break
            head = self._pending[:cut]
            waited = self._clock() - self._last_sent_at
            if len(head.strip()) < self._min_chars and waited < self._max_wait:
                # 段落太短：尝试把下一个段落一起带上，否则等后续内容。
                nxt = self._pending.find("\n\n", cut + 2)
                if nxt < 0:
                    break
                head = self._pending[:nxt]
                cut = nxt
            self._pending = self._pending[cut + 2:]
            plain = to_plain(head)
            if plain:
                for part in split_long(plain, self._max_chars, self.remaining - 1):
                    self._emit(part)

    def finish(self, fallback: Optional[str] = None) -> None:
        """发出剩余内容；整轮没有任何正文时发送 ``fallback``。"""
        tail = to_plain(self._pending)
        self._pending = ""
        if not tail and not self._any_text and fallback:
            tail = fallback.strip()
        if not tail:
            return
        for part in split_long(tail, self._max_chars, max(1, self.remaining)):
            self._emit(part)
