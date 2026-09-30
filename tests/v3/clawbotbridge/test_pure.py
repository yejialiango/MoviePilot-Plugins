"""clawbotbridge 中不依赖宿主的纯逻辑：分段发送与 iLink 报文解析。

两个模块都不 import ``app.*``，这里按文件路径直接加载，可脱离 MoviePilot 后端单独运行：
``pytest --noconftest tests/v3/clawbotbridge/test_pure.py``。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[3] / "plugins.v3" / "clawbotbridge"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"clawbotbridge_{name}", PLUGIN_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclass 需要能在 sys.modules 里找到所属模块
    spec.loader.exec_module(module)
    return module


seg_mod = _load("segmenter")
ilink_mod = _load("ilink")


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _segmenter(budget=8, min_chars=10, max_chars=1800, max_wait=8.0, fail=False):
    sent = []
    clock = FakeClock()

    def send(text: str) -> bool:
        if fail:
            return False
        sent.append(text)
        return True

    seg = seg_mod.Segmenter(send, budget=budget, min_chars=min_chars, max_chars=max_chars,
                            max_wait=max_wait, clock=clock)
    return seg, sent, clock


def test_paragraphs_are_sent_as_they_complete():
    seg, sent, _ = _segmenter()
    seg.feed("第一段内容已经足够长了。\n\n第二段")
    assert sent == ["第一段内容已经足够长了。"]
    seg.feed("还在继续写")
    assert len(sent) == 1
    seg.finish()
    assert sent == ["第一段内容已经足够长了。", "第二段还在继续写"]


def test_short_paragraph_waits_and_merges_with_next():
    seg, sent, _ = _segmenter(min_chars=20)
    seg.feed("短。\n\n")
    assert sent == []
    seg.feed("第二段也写完了，合起来够长。\n\n")
    assert sent == ["短。\n\n第二段也写完了，合起来够长。"]


def test_short_paragraph_is_flushed_after_max_wait():
    seg, sent, clock = _segmenter(min_chars=100, max_wait=5)
    seg.feed("短。\n\n")
    assert sent == []
    clock.now = 6
    seg.feed("后续")
    assert sent == ["短。"]


def test_budget_reserves_last_bubble_for_finish():
    seg, sent, _ = _segmenter(budget=3, min_chars=1)
    for i in range(10):
        seg.feed(f"段落{i}\n\n")
    # 中途最多发 budget-1 条，最后一条留给收尾。
    assert len(sent) == 2
    seg.finish()
    assert len(sent) == 3
    assert "段落9" in sent[-1] and "段落2" in sent[-1]


def test_finish_never_exceeds_budget_for_long_tail():
    seg, sent, _ = _segmenter(budget=2, min_chars=1, max_chars=20)
    seg.feed("\n".join(f"第{i}行内容比较长一些" for i in range(30)))
    seg.finish()
    assert len(sent) == 2


def test_progress_is_capped_and_leaves_room_for_text():
    seg, sent, _ = _segmenter(budget=4)
    assert seg.progress("⏳ 搜索中") is True
    assert seg.progress("⏳ 再搜索") is True
    assert seg.progress("⏳ 第三次") is False  # 默认最多 2 条进度
    assert seg.remaining == 2


def test_progress_refused_when_budget_tight():
    seg, sent, _ = _segmenter(budget=2)
    assert seg.progress("⏳ 搜索中") is False
    assert sent == []


def test_failed_send_does_not_consume_budget():
    seg, sent, _ = _segmenter(fail=True)
    seg.feed("一段足够长的内容。\n\n")
    assert seg.sent == 0


def test_finish_uses_fallback_only_without_text():
    seg, sent, _ = _segmenter()
    seg.finish(fallback="⚠️ 出错了")
    assert sent == ["⚠️ 出错了"]
    seg2, sent2, _ = _segmenter()
    seg2.feed("正文")
    seg2.finish(fallback="⚠️ 出错了")
    assert sent2 == ["正文"]


def test_to_plain_strips_markdown():
    text = "## 标题\n\n**加粗** 和 [链接](https://a.b/c)\n```python\ncode\n```"
    assert seg_mod.to_plain(text) == "标题\n\n加粗 和 链接 https://a.b/c\ncode"


def test_line_filter_splits_summary_lines_across_deltas():
    texts, summaries = [], []
    lf = seg_mod.LineFilter(texts.append, summaries.append)
    lf.feed("好的，我查一下。\n（执行了 ")
    lf.feed("2 次搜索）\n结果如下")
    lf.flush()
    assert summaries == ["（执行了 2 次搜索）"]
    assert "".join(texts) == "好的，我查一下。\n结果如下"


def test_parse_inbound_text_and_voice():
    msg = {
        "message_id": 7, "from_user_id": "u@im.wechat", "message_type": 1, "context_token": "ctx",
        "item_list": [{"type": 1, "text_item": {"text": "你好"}},
                      {"type": 3, "voice_item": {"text": "语音转写"}}],
    }
    inbound = ilink_mod.ILinkClient.parse_inbound(msg)
    assert inbound.text == "你好\n语音转写"
    assert inbound.context_token == "ctx"
    assert inbound.message_id == "7"


def test_parse_inbound_ignores_bot_and_empty_messages():
    parse = ilink_mod.ILinkClient.parse_inbound
    assert parse({"message_type": 2, "from_user_id": "b", "item_list": [
        {"type": 1, "text_item": {"text": "机器人自己发的"}}]}) is None
    assert parse({"message_type": 1, "from_user_id": "u", "item_list": [{"type": 2}]}) is None


def test_check_raises_with_code():
    try:
        ilink_mod.ILinkClient._check("getupdates", {"ret": -14, "errmsg": "stale"})
    except ilink_mod.ILinkError as err:
        assert err.code == -14
    else:
        raise AssertionError("expected ILinkError")


def test_segmenter_keeps_markdown_tables():
    seg, sent, _ = _segmenter(min_chars=1)
    table = "| 时间 | 片名 |\n|---|---|\n| 09-29 | **小妇人** |"
    seg.feed(f"结果如下：\n\n{table}\n\n")
    seg.finish()
    assert sent == ["结果如下：", table]
