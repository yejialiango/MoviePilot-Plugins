"""扫码分享页：免登录打开，持续显示当前有效的二维码，过期换新后自动刷新。

页面每 3 秒拉一次 ``share_status``，只凭链接里的一次性 token 访问；扫码会话结束（已连接、
取消或超出扫码窗口）后 token 即失效。
"""

from __future__ import annotations

import html

SHARE_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>扫码接入微信 ClawBot</title>
<style>
  :root { --bg:#f5f5f7; --card:#fff; --fg:#1d1d1f; --muted:#6e6e73; --ok:#1a7f37; --warn:#b35900; --err:#c0392b; --accent:#07c160; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#111113; --card:#1c1c1e; --fg:#f2f2f7; --muted:#a1a1a6; --ok:#3fb950; --warn:#e3a008; --err:#ff6b5e; }
  }
  * { box-sizing: border-box; }
  body { margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center;
         background:var(--bg); color:var(--fg); font:15px/1.6 -apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif; padding:16px; }
  .card { background:var(--card); border-radius:16px; padding:24px; width:100%; max-width:380px; text-align:center;
          box-shadow:0 4px 24px rgba(0,0,0,.08); }
  h1 { font-size:18px; margin:0 0 4px; }
  .sub { color:var(--muted); font-size:13px; margin:0 0 16px; }
  .qr { width:260px; height:260px; margin:0 auto; border-radius:12px; background:#fff; padding:8px;
        display:flex; align-items:center; justify-content:center; }
  .qr img { width:100%; height:100%; image-rendering:pixelated; }
  .qr.dim img { opacity:.15; }
  .status { margin-top:16px; font-weight:600; }
  .status.ok { color:var(--ok); } .status.warn { color:var(--warn); } .status.err { color:var(--err); }
  .meta { color:var(--muted); font-size:13px; margin-top:6px; }
  ol { text-align:left; color:var(--muted); font-size:13px; padding-left:20px; margin:16px 0 0; }
</style>
</head>
<body>
<div class="card">
  <h1>扫码接入 MoviePilot 助手</h1>
  <p class="sub">__TITLE__</p>
  <div class="qr" id="qr"><span class="meta">加载中…</span></div>
  <div class="status" id="status">正在获取二维码…</div>
  <div class="meta" id="meta"></div>
  <ol>
    <li>用要接入的微信「扫一扫」上面的二维码</li>
    <li>在手机上点确认</li>
    <li>二维码约 4 分钟过期，本页会自动换成新的，无需刷新</li>
  </ol>
</div>
<script>
const token = new URLSearchParams(location.search).get("t") || "";
const qr = document.getElementById("qr"), st = document.getElementById("status"), meta = document.getElementById("meta");
let data = null, timer = null;
function fmt(s) { s = Math.max(0, Math.floor(s)); return Math.floor(s / 60) + ":" + String(s % 60).padStart(2, "0"); }
function render() {
  if (!data) return;
  const now = Date.now() / 1000;
  st.className = "status " + (data.tone || "");
  st.textContent = data.message;
  if (data.qr_image && !data.done) {
    if (!qr.querySelector("img") || qr.querySelector("img").src !== data.qr_image) {
      qr.innerHTML = '<img alt="二维码" src="' + data.qr_image + '">';
    }
    qr.classList.toggle("dim", data.status === "scaned");
    meta.textContent = "当前二维码剩余 " + fmt(data.qr_expires_at - now) + " · 第 " + data.qr_index + " 张 · 扫码窗口剩余 " + fmt(data.window_ends_at - now);
  } else {
    qr.innerHTML = data.done && data.status === "confirmed" ? '<span style="font-size:64px">✅</span>' : '<span class="meta">—</span>';
    meta.textContent = data.done ? "可以关闭本页" : "";
  }
}
async function poll() {
  try {
    const r = await fetch("share_status?t=" + encodeURIComponent(token), { cache: "no-store" });
    data = await r.json();
  } catch (e) { data = { message: "网络异常，稍后重试…", tone: "warn" }; }
  render();
  if (!data.done) setTimeout(poll, 3000);
}
timer = setInterval(render, 1000);
poll();
</script>
</body>
</html>
"""

INVALID_HTML = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>链接已失效</title></head>
<body style="font-family:-apple-system,'PingFang SC',sans-serif;text-align:center;padding:48px 16px;color:#6e6e73">
<h2 style="color:inherit">链接已失效</h2><p>扫码已结束或已取消，请让管理员重新生成二维码。</p></body></html>
"""


def render_share_page(title: str) -> str:
    return SHARE_HTML.replace("__TITLE__", html.escape(title))
