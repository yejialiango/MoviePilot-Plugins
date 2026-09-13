"""整理前字幕 —— 探针版。

目标形态是在 MoviePilot 把文件搬到目标存储之前，趁源文件还在本地盘时完成字幕
生成/翻译，让字幕随视频一次入库。本版本只做观测，默认不干预任何整理行为：

* 订阅 ChainEventType.TransferIntercept，记录宿主在「写副作用前」交给插件的完整上下文；
* 订阅 TransferComplete / TransferFailed，观察一次整理的最终落点；
* 可选的「拦截演练」对单个文件做一次 cancel，用来实测取消整理后的宿主行为，
  以及 TransferChain().do_transfer() 重投是否能闭环。

探针要回答的三个问题（详见 README）：
1. cancel 之后宿主把它记成失败还是待重试，会不会推送通知；
2. 同一个文件两次进入拦截时，指纹是否稳定（防重入的基础）；
3. meta 字段在 over_flag 为 None 的分支里不会被赋值，实际拿到的是不是 None。
"""

from __future__ import annotations

import hashlib
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.plugins import _PluginBase
from app.schemas.types import ChainEventType, EventType, NotificationType
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger

# 源文件落在这些前缀下，才算「还在本地盘」。CloudDrive2 的挂载点单独列出来做反向判断，
# 用于确认拦截时刻拿到的确实是本地路径而不是网盘路径。
LOCAL_HINTS = ("/CloudNAS/downloads", "/downloads", "/volume1", "/volume4")
CLOUD_HINTS = ("/CloudNAS/115", "/115/")


class PreTransferSub(_PluginBase):
    """在整理搬运前介入的字幕预处理插件（当前为观测探针）。"""

    plugin_name = "整理前字幕"
    plugin_desc = "整理搬运前趁文件还在本地生成字幕，随视频一并入库。当前版本为观测探针。"
    plugin_icon = "https://raw.githubusercontent.com/yejialiango/MoviePilot-Plugins/main/icons/pretransfersub.png"
    plugin_version = "0.1.1"
    plugin_author = "yejialiango"
    author_url = "https://github.com/yejialiango"
    plugin_config_prefix = "pretransfersub_"
    plugin_order = 21
    auth_level = 1

    # 观测记录上限，避免 plugindata 无限膨胀。
    MAX_RECORDS = 200
    # 演练重投前的等待秒数，给宿主结算留出时间，避免与本次取消的收尾抢同一条历史。
    REDO_DELAY = 5

    def __init__(self) -> None:
        super().__init__()
        self._enabled = False
        self._observe_only = True
        self._drill_enabled = False
        self._drill_keyword = ""
        self._drill_redo = False
        self._notify = False
        self._records: List[dict] = []
        self._seen: Dict[str, dict] = {}
        self._lock = threading.Lock()

    def init_plugin(self, config: Optional[dict] = None) -> None:
        if config:
            self._enabled = config.get("enabled", False)
            self._observe_only = config.get("observe_only", True)
            self._drill_enabled = config.get("drill_enabled", False)
            self._drill_keyword = (config.get("drill_keyword") or "").strip()
            self._drill_redo = config.get("drill_redo", False)
            self._notify = config.get("notify", False)

            if config.get("clear_records"):
                config["clear_records"] = False
                self.update_config(config)
                self.save_data("records", [])
                self.save_data("seen", {})
                logger.info(f"【{self.plugin_name}】观测记录已清空")

        self._records = self.get_data("records") or []
        self._seen = self.get_data("seen") or {}

        if self._enabled:
            mode = "观测" if self._observe_only else "干预"
            drill = f"，演练拦截已开启（关键字：{self._drill_keyword or '<未填，不生效>'}）" if self._drill_enabled else ""
            logger.info(f"【{self.plugin_name}】已启用，当前为{mode}模式{drill}")

    def get_state(self) -> bool:
        return self._enabled

    # region 事件处理

    @eventmanager.register(ChainEventType.TransferIntercept)
    def on_transfer_intercept(self, event: Event = None):
        """整理搬运前的拦截点。探针默认只记录，不改写 cancel。"""
        if not self._enabled or not event or not event.event_data:
            return
        try:
            data = event.event_data
            fileitem = data.fileitem
            src_path = str(getattr(fileitem, "path", "") or "")
            key = self.__fingerprint(fileitem)

            with self._lock:
                seen = self._seen.get(key) or {"count": 0, "drilled": False}
                seen["count"] += 1
                seq = seen["count"]

            decision = "观测放行"
            # 演练：命中关键字且此前没演练过的文件，取消一次整理。
            if (
                not self._observe_only
                and self._drill_enabled
                and self._drill_keyword
                and self._drill_keyword in src_path
                and not seen.get("drilled")
            ):
                data.cancel = True
                data.source = self.plugin_name
                data.reason = "整理前字幕：拦截演练"
                seen["drilled"] = True
                decision = "演练拦截(cancel=True)"
                if self._drill_redo:
                    self.__schedule_redo(data)
                    decision += " + 已排重投"
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.Plugin,
                        title=f"【{self.plugin_name}】拦截演练",
                        text=f"文件：{getattr(fileitem, 'name', '')}\n已取消本次整理，用于观测宿主行为",
                    )

            with self._lock:
                self._seen[key] = seen

            record = {
                "ts": datetime.now().strftime("%m-%d %H:%M:%S"),
                "stage": "拦截",
                "key": key[:10],
                "seq": seq,
                "name": getattr(fileitem, "name", ""),
                "src": src_path,
                "src_side": self.__which_side(src_path),
                "size": getattr(fileitem, "size", None),
                "mtime": getattr(fileitem, "modify_time", None),
                "storage": getattr(fileitem, "storage", ""),
                "meta_is_none": data.meta is None,
                "meta_kind": type(data.meta).__name__ if data.meta is not None else "-",
                "media": self.__media_brief(data.mediainfo),
                "target_storage": data.target_storage,
                "target_path": str(data.target_path or ""),
                "target_side": self.__which_side(str(data.target_path or "")),
                "transfer_type": data.transfer_type,
                "options": str(data.options or ""),
                "decision": decision,
            }
            self.__add_record(record)
            logger.info(
                f"【{self.plugin_name}】拦截观测 #{seq} key={key[:10]} "
                f"{record['name']} | 源={record['src_side']} | 目标={record['target_side']} | "
                f"方式={data.transfer_type} | meta={'None' if record['meta_is_none'] else record['meta_kind']} | "
                f"{decision}"
            )
        except Exception as e:
            # 拦截点在整理主链路上，任何异常都必须就地吞掉，绝不能影响宿主整理。
            logger.error(f"【{self.plugin_name}】拦截观测异常：{e}")

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event = None):
        """整理完成，记录最终落点，用于和拦截记录对账。"""
        self.__record_settlement(event, stage="完成")

    @eventmanager.register(EventType.TransferFailed)
    def on_transfer_failed(self, event: Event = None):
        """整理失败。演练取消后是否走这里，是探针要回答的核心问题。"""
        self.__record_settlement(event, stage="失败")

    # endregion

    # region 私有方法

    def __record_settlement(self, event: Optional[Event], stage: str) -> None:
        if not self._enabled or not event or not event.event_data:
            return
        try:
            item = event.event_data
            transferinfo = item.get("transferinfo") if isinstance(item, dict) else None
            mediainfo = item.get("mediainfo") if isinstance(item, dict) else None
            file_list_new = getattr(transferinfo, "file_list_new", None) or []
            first = file_list_new[0] if file_list_new else ""
            self.__add_record({
                "ts": datetime.now().strftime("%m-%d %H:%M:%S"),
                "stage": stage,
                "key": "-",
                "seq": "-",
                "name": getattr(getattr(transferinfo, "fileitem", None), "name", ""),
                "src": str(getattr(getattr(transferinfo, "fileitem", None), "path", "") or ""),
                "src_side": "-",
                "media": self.__media_brief(mediainfo),
                "target_path": first,
                "target_side": self.__which_side(first),
                "transfer_type": getattr(transferinfo, "transfer_type", ""),
                "decision": f"success={getattr(transferinfo, 'success', '?')} "
                            f"msg={getattr(transferinfo, 'message', '') or '-'} "
                            f"files={len(file_list_new)}",
            })
            logger.info(
                f"【{self.plugin_name}】整理{stage}：{getattr(transferinfo, 'message', '') or '-'}"
            )
        except Exception as e:
            logger.error(f"【{self.plugin_name}】结算观测异常：{e}")

    def __schedule_redo(self, data: Any) -> None:
        """在后台线程里用宿主公开入口把这次取消的整理重新投一遍。"""
        fileitem = data.fileitem
        meta = data.meta
        mediainfo = data.mediainfo
        target_storage = data.target_storage
        target_path = data.target_path
        transfer_type = data.transfer_type

        def _redo():
            time.sleep(self.REDO_DELAY)
            try:
                from app.chain.transfer.facade import TransferChain

                state, msg = TransferChain().do_transfer(
                    fileitem=fileitem,
                    meta=meta,
                    mediainfo=mediainfo,
                    target_storage=target_storage,
                    target_path=target_path,
                    transfer_type=transfer_type,
                    background=True,
                )
                logger.info(f"【{self.plugin_name}】重投结果 state={state} msg={msg}")
                self.__add_record({
                    "ts": datetime.now().strftime("%m-%d %H:%M:%S"),
                    "stage": "重投",
                    "key": "-",
                    "seq": "-",
                    "name": getattr(fileitem, "name", ""),
                    "src": str(getattr(fileitem, "path", "") or ""),
                    "decision": f"do_transfer state={state} msg={msg}",
                })
            except Exception as e:
                logger.error(f"【{self.plugin_name}】重投异常：{e}")

        threading.Thread(target=_redo, daemon=True, name="pretransfersub-redo").start()

    @staticmethod
    def __fingerprint(fileitem: Any) -> str:
        """路径 + 大小 + 修改时间，作为同一个文件跨多次拦截的稳定标识。"""
        raw = "|".join([
            str(getattr(fileitem, "path", "") or ""),
            str(getattr(fileitem, "size", "") or ""),
            str(getattr(fileitem, "modify_time", "") or ""),
        ])
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def __which_side(path: str) -> str:
        """判断路径落在本地盘还是网盘挂载，用于验证拦截时刻源文件确实还在本地。"""
        if not path:
            return "-"
        if any(h in path for h in CLOUD_HINTS):
            return "网盘"
        if any(path.startswith(h) or h in path for h in LOCAL_HINTS):
            return "本地"
        return "未知"

    @staticmethod
    def __media_brief(mediainfo: Any) -> str:
        if not mediainfo:
            return "-"
        title = getattr(mediainfo, "title", "") or ""
        year = getattr(mediainfo, "year", "") or ""
        mtype = getattr(mediainfo, "type", "")
        mtype = getattr(mtype, "value", mtype) or ""
        tmdbid = getattr(mediainfo, "tmdb_id", "") or ""
        lang = getattr(mediainfo, "original_language", "") or ""
        return f"{title} ({year}) [{mtype}] tmdb={tmdbid} lang={lang}"

    def __add_record(self, record: dict) -> None:
        with self._lock:
            self._records.insert(0, record)
            self._records = self._records[: self.MAX_RECORDS]
            records = list(self._records)
            seen = dict(self._seen)
        self.save_data("records", records)
        self.save_data("seen", seen)

    # endregion

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "enabled", "label": "启用插件"},
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "observe_only",
                                        "label": "只观测不干预",
                                        "hint": "开启时绝不改写 cancel，整理行为与未装插件完全一致",
                                    },
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "notify", "label": "演练时发通知"},
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "clear_records", "label": "清空观测记录"},
                                }],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "drill_enabled",
                                        "label": "拦截演练",
                                        "hint": "需同时关闭「只观测不干预」才生效",
                                    },
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "drill_keyword",
                                        "label": "演练路径关键字",
                                        "placeholder": "例如某部片子的目录名，命中才拦截一次",
                                    },
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "drill_redo",
                                        "label": "演练后自动重投",
                                        "hint": "取消后调 do_transfer 重新提交，验证闭环",
                                    },
                                }],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": "探针版：默认只记录 TransferIntercept 的上下文，不改变任何整理行为。"
                                            "演练拦截只对命中关键字的文件生效一次，用于实测取消整理后的宿主表现。",
                                },
                            }],
                        }],
                    },
                ],
            }
        ], {
            "enabled": False,
            "observe_only": True,
            "drill_enabled": False,
            "drill_keyword": "",
            "drill_redo": False,
            "notify": False,
            "clear_records": False,
        }

    def get_page(self) -> List[dict]:
        records = self.get_data("records") or []
        if not records:
            return [{
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal", "text": "暂无观测记录。启用插件后，下一次整理就会记录。"},
            }]
        headers = ["时间", "阶段", "指纹", "第N次", "文件", "源位置", "媒体信息",
                   "目标位置", "方式", "meta", "决策/结果"]
        rows = []
        for r in records:
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "text": r.get("ts", "")},
                    {"component": "td", "text": r.get("stage", "")},
                    {"component": "td", "text": r.get("key", "")},
                    {"component": "td", "text": str(r.get("seq", ""))},
                    {"component": "td", "text": r.get("name", "")},
                    {"component": "td", "text": f"{r.get('src_side', '')} {r.get('src', '')}"},
                    {"component": "td", "text": r.get("media", "")},
                    {"component": "td", "text": f"{r.get('target_side', '')} {r.get('target_path', '')}"},
                    {"component": "td", "text": r.get("transfer_type", "")},
                    {"component": "td", "text": r.get("meta_kind", "-")},
                    {"component": "td", "text": r.get("decision", "")},
                ],
            })
        return [{
            "component": "VTable",
            "props": {"hover": True, "density": "compact"},
            "content": [
                {"component": "thead", "content": [{
                    "component": "tr",
                    "content": [{"component": "th", "text": h} for h in headers],
                }]},
                {"component": "tbody", "content": rows},
            ],
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def stop_service(self) -> None:
        """探针没有常驻线程，重投线程是 daemon，进程退出即回收。"""
        pass
