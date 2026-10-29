# -*- coding: utf-8 -*-
"""M5: Streamlit GUI —— 数据库浏览器 + AI 按需筛选 + 流水线控制台。

启动方式:
    python -m streamlit run app.py
    python -m radar gui

设计约束:
- 后台子进程只向线程安全的 queue.Queue 投递消息，绝不跨线程调用 st.*
- 主线程 / 片段消费队列，更新日志与运行状态
- 数据库只读渲染，写操作（收藏/刷新）通过显式按钮触发
- 不做任何预先打分，全部原始数据保留，筛选交给用户与按需 AI
"""
import html
import json
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

import streamlit as st

from radar import database, prompts, store, telemetry, viewmodel
from radar.config import DEFAULTS, PROJECT_ROOT, load_config, save_config, validate_config
from radar.pipeline import EXIT_RETRYABLE
from radar.store import DATA_DIR, RUNS_DIR

# Windows asyncio ProactorEventLoop 的已知噪音：对端强制断开（如关闭/刷新
# Streamlit 页面、网络抖动）后，连接收尾时 socket.shutdown() 会抛
# ConnectionResetError，被默认异常处理器打印成 _call_connection_lost 回调错误。
if sys.platform == "win32":
    try:
        from asyncio.proactor_events import _ProactorBasePipeTransport

        _original_call_connection_lost = _ProactorBasePipeTransport._call_connection_lost

        def _quiet_call_connection_lost(self, exc):
            try:
                _original_call_connection_lost(self, exc)
            except ConnectionResetError:
                pass

        _ProactorBasePipeTransport._call_connection_lost = _quiet_call_connection_lost
    except Exception:
        pass

st.set_page_config(
    page_title="纯血鸿蒙机会雷达",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------------------------------------------------------------------
# 设计 token 与全局样式（只保留两个信号色：达标=青，生态空白=琥珀）
# --------------------------------------------------------------------------
CSS = """
<style>
:root{
  color-scheme:dark;
  --bg-0:#05070C; --bg-1:#0A0D12; --surface-1:#0F131C; --surface-2:#141A26;
  --border-1:#1E2735; --border-2:#2A3646;
  --text-1:#E7ECF3; --text-2:#9AA7B8; --text-3:#8A99AC;
  --cyan:#38BDF8; --green:#6EE7B7; --amber:#E9A568; --gold:#FFD54F;
  --danger:#FF8A80; --ok:#6EE7B7;
  --radius:14px; --radius-sm:10px; --ease:cubic-bezier(.22,.61,.36,1);
  --font-display:Georgia,"Times New Roman",serif;
  --font-mono:ui-monospace,"Cascadia Mono",Consolas,monospace;
}
html{ color-scheme:dark; }
.stApp{ background:var(--bg-0); }
.block-container{ padding-top:4.4rem; padding-bottom:2.5rem; max-width:1500px; }
h1,h2,h3,h4{ color:var(--text-1); letter-spacing:.2px; text-wrap:balance; }
p, li, span, label{ color:var(--text-2); }
::selection{ background:rgba(56,189,248,.30); color:var(--text-1); }
input, textarea{ caret-color:var(--cyan); }
::-webkit-scrollbar{ width:10px; height:10px; }
::-webkit-scrollbar-track{ background:var(--bg-1); }
::-webkit-scrollbar-thumb{ background:#26303F; border-radius:999px; border:2px solid var(--bg-1); }
*{ scrollbar-color:#26303F var(--bg-1); scrollbar-width:thin; }
:focus-visible{ outline:2px solid var(--cyan); outline-offset:2px; border-radius:4px; }
header[data-testid="stHeader"]{ background:transparent !important; }
section[data-testid="stSidebar"]{ background:var(--bg-1) !important; border-right:1px solid var(--border-1); }

.brand{ display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
.brand-mark{ width:38px; height:38px; flex:none; color:var(--cyan); }
.brand-mark svg{ display:block; width:100%; height:100%; }
.brand-title{ font-family:var(--font-display); font-size:1.72rem; font-weight:700;
  color:var(--text-1); line-height:1.12; letter-spacing:.4px; }
.brand-sub{ font-size:.84rem; color:var(--text-3); margin-top:3px; }
.brand-meta{ display:flex; gap:0; flex-wrap:wrap; margin-top:11px; align-items:baseline; }
.brand-meta .item{ font-family:var(--font-mono); font-size:.76rem; color:var(--text-3);
  padding-right:14px; margin-right:14px; border-right:1px solid var(--border-1); }
.brand-meta .item:last-child{ border-right:none; margin-right:0; padding-right:0; }
.brand-meta .item b{ color:var(--text-1); font-weight:650; }

.status{ display:inline-flex; align-items:center; gap:7px; font-size:.78rem; font-weight:600;
  padding:4px 12px; border-radius:999px; border:1px solid var(--border-2); }
.status .dot{ width:7px; height:7px; border-radius:50%; background:var(--text-3); }
.status.idle{ color:var(--text-2); }
.status.running{ color:var(--cyan); border-color:rgba(56,189,248,.45); background:rgba(56,189,248,.08); }
.status.running .dot{ background:var(--cyan); animation:pulse 1.4s var(--ease) infinite; }
.status.done{ color:var(--ok); border-color:rgba(110,231,183,.4); background:rgba(110,231,183,.07); }
.status.done .dot{ background:var(--ok); }
.status.failed{ color:var(--danger); border-color:rgba(255,138,128,.45); background:rgba(255,138,128,.08); }
.status.failed .dot{ background:var(--danger); }
.status.interrupted{ color:var(--amber); border-color:rgba(233,165,104,.45); background:rgba(233,165,104,.08); }
.status.interrupted .dot{ background:var(--amber); }
@keyframes pulse{ 0%,100%{opacity:1} 50%{opacity:.3} }
@media (prefers-reduced-motion:reduce){ *{ animation:none !important; transition:none !important; } }

.panel{ background:var(--surface-1); border:1px solid var(--border-1); border-radius:var(--radius);
  padding:16px 18px; margin-bottom:12px; }
.panel-title{ font-size:.92rem; font-weight:700; color:var(--text-1); margin-bottom:10px; }

.statline{ display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr));
  background:var(--surface-1); border:1px solid var(--border-1); border-radius:var(--radius); }
.stat{ padding:13px 16px; border-left:1px solid var(--border-1); min-width:0; }
.stat:first-child{ border-left:none; }
.stat-k{ font-size:.76rem; color:var(--text-3); }
.stat-v{ font-size:1.7rem; font-weight:700; line-height:1.15; margin-top:3px;
  color:var(--text-1); font-variant-numeric:tabular-nums; }
.stat-v.sig{ color:var(--cyan); }
.stat-v.gap{ color:var(--gold); }
.stat-s{ font-size:.73rem; color:var(--text-3); margin-top:2px; }
@media (max-width:760px){
  .statline{ grid-template-columns:repeat(2,minmax(0,1fr)); }
  .stat{ border-left:none; border-top:1px solid var(--border-1); }
  .stat:nth-child(-n+2){ border-top:none; }
}

.pill{ display:inline-block; font-size:.68rem; padding:2px 9px; border-radius:999px;
  border:1px solid var(--border-2); color:var(--text-2); }
.pill-blank{ color:var(--gold); border-color:rgba(255,213,79,.4); background:rgba(255,213,79,.08); }
.pill-refresh{ color:var(--amber); border-color:rgba(233,165,104,.4); background:rgba(233,165,104,.08); }
.pill-fav{ color:var(--cyan); border-color:rgba(56,189,248,.4); background:rgba(56,189,248,.08); }

.detail-head{ display:flex; align-items:center; gap:12px; flex-wrap:wrap; }
.detail-name{ font-family:var(--font-display); font-size:1.34rem; font-weight:700;
  color:var(--text-1); line-height:1.2; }
.muted{ color:var(--text-3); font-size:.82rem; line-height:1.55; }
.facts{ display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:10px; margin-top:6px; }
.fact{ background:var(--surface-2); border:1px solid var(--border-1); border-radius:var(--radius-sm);
  padding:10px 12px; }
.fact-k{ font-size:.72rem; color:var(--text-3); }
.fact-v{ font-size:1.05rem; font-weight:650; color:var(--text-1); margin-top:2px;
  font-variant-numeric:tabular-nums; }
.comp-row{ padding:7px 0 7px 12px; margin-bottom:8px; border-left:1px solid var(--border-2); }
.comp-name{ color:var(--text-1); font-weight:600; font-size:.9rem; }
.comp-meta{ color:var(--text-3); font-size:.76rem; font-variant-numeric:tabular-nums; }
.comp-meta span{ margin-right:14px; }
.pain{ color:var(--amber); font-size:.78rem; margin-top:4px; }
.verdict{ background:var(--surface-2); border:1px solid var(--border-1);
  border-radius:var(--radius-sm); padding:12px 14px; color:var(--text-1); font-size:.9rem;
  line-height:1.6; }

.st-key-detail_card, [class*="st-key-gap_card_"], [class*="st-key-opp_card_"]{
  background:var(--surface-1); border:1px solid var(--border-1); border-radius:var(--radius);
  padding:18px 20px; margin-bottom:14px; }
[class*="st-key-gap_card_"]{
  background:linear-gradient(90deg, rgba(233,165,104,.06), transparent 40%), var(--surface-1); }

.artifact{ display:flex; align-items:center; justify-content:space-between; gap:10px;
  padding:9px 12px; border-bottom:1px solid var(--border-1); font-size:.82rem; }
.artifact:last-child{ border-bottom:none; }
.artifact .name{ color:var(--text-1); font-weight:600; }
.artifact .meta{ color:var(--text-3); font-size:.76rem; text-align:right; }
.artifact .state{ display:inline-flex; align-items:center; gap:6px; font-size:.76rem; }
.artifact .state .dot{ width:6px; height:6px; border-radius:50%; background:currentColor; }
.artifact .state.ok{ color:var(--ok); }
.artifact .state.bad{ color:var(--danger); }

.logbox{ background:#04060A; border:1px solid var(--border-1); border-radius:var(--radius-sm);
  padding:12px 14px; font-family:var(--font-mono); font-size:.76rem;
  line-height:1.55; color:#B9C6D6; max-height:420px; overflow-y:auto; white-space:pre-wrap;
  word-break:break-word; overscroll-behavior:contain; }
.logbox.small{ max-height:150px; }

.prog{ margin-top:12px; }
.prog-head{ display:flex; justify-content:space-between; align-items:baseline; gap:12px;
  font-size:.8rem; margin-bottom:7px; }
.prog-label{ color:var(--text-1); font-weight:650; }
.prog-stage{ color:var(--text-3); font-family:var(--font-mono); font-size:.74rem; }
.prog-pct{ color:var(--cyan); font-family:var(--font-mono); font-variant-numeric:tabular-nums;
  font-weight:700; }
.prog-track{ height:9px; border-radius:999px; background:var(--surface-2);
  overflow:hidden; border:1px solid var(--border-1); }
.prog-fill{ height:100%; border-radius:999px; position:relative;
  background:linear-gradient(90deg,#38BDF8,#6EE7B7);
  transition:width .6s cubic-bezier(.16,1,.3,1);
  box-shadow:0 0 14px rgba(56,189,248,.35); }
.prog-fill::after{ content:""; position:absolute; inset:0;
  background:linear-gradient(90deg,transparent,rgba(255,255,255,.55),transparent);
  background-size:200% 100%; animation:shimmer 1.8s linear infinite; }
@keyframes shimmer{ 0%{background-position:200% 0} 100%{background-position:-200% 0} }
.prog-meta{ color:var(--text-3); font-size:.74rem; margin-top:6px; font-family:var(--font-mono); }
@media (prefers-reduced-motion:reduce){
  .prog-fill{ transition:none; } .prog-fill::after{ animation:none; }
}

.trace{ background:var(--surface-1); border:1px solid var(--border-1);
  border-radius:var(--radius-sm); padding:10px 13px; margin-bottom:8px; }
.trace-head{ display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
.trace-stage{ font-family:var(--font-mono); font-size:.72rem; color:var(--text-3);
  border:1px solid var(--border-2); border-radius:999px; padding:1px 9px; }
.trace-target{ color:var(--text-1); font-weight:650; font-size:.86rem; }
.trace-dur{ color:var(--text-3); font-family:var(--font-mono); font-size:.74rem;
  margin-left:auto; font-variant-numeric:tabular-nums; }
.trace-st{ font-size:.72rem; font-weight:650; }
.trace-st.ok{ color:var(--ok); } .trace-st.err{ color:var(--danger); }
.trace-body{ margin-top:8px; }
.trace-sec{ margin-top:8px; }
.trace-sec .k{ font-size:.72rem; color:var(--text-3); margin-bottom:3px; }
.trace-pre{ background:#04060A; border:1px solid var(--border-1); border-radius:8px;
  padding:9px 11px; font-family:var(--font-mono); font-size:.73rem; color:#B9C6D6;
  max-height:260px; overflow:auto; white-space:pre-wrap; word-break:break-word; }

.prompt-head{ display:flex; align-items:center; gap:10px; flex-wrap:wrap; margin-bottom:6px; }
.prompt-name{ color:var(--text-1); font-weight:650; }
.ph-chip{ font-family:var(--font-mono); font-size:.7rem; color:var(--cyan);
  background:rgba(56,189,248,.08); border:1px solid rgba(56,189,248,.3);
  border-radius:6px; padding:1px 7px; margin-right:5px; }
.custom-badge{ font-size:.68rem; color:var(--gold); border:1px solid rgba(255,213,79,.4);
  background:rgba(255,213,79,.08); border-radius:999px; padding:1px 9px; }

.empty{ border:1px dashed var(--border-2); border-radius:var(--radius); padding:26px 22px;
  text-align:center; background:var(--surface-1); }
.empty .t{ color:var(--text-1); font-weight:650; font-size:.98rem; }
.empty .d{ color:var(--text-3); font-size:.82rem; margin-top:6px; }
.empty .cmd{ display:inline-block; margin-top:10px; background:var(--bg-0); border:1px solid var(--border-2);
  border-radius:8px; padding:5px 11px; color:var(--cyan); font-size:.78rem;
  font-family:var(--font-mono); }

.stTabs [data-baseweb="tab-list"]{ gap:6px; border-bottom:1px solid var(--border-1); }
.stTabs [data-baseweb="tab"]{ padding:9px 16px; border-radius:10px 10px 0 0; color:var(--text-2);
  transition:color .18s var(--ease), background .18s var(--ease); }
.stTabs [data-baseweb="tab"]:hover{ color:var(--text-1); }
.stTabs [aria-selected="true"]{ color:var(--text-1) !important; background:var(--surface-1); font-weight:650; }
div[data-testid="stExpander"] details{ border:1px solid var(--border-1) !important; border-radius:var(--radius-sm) !important; }
hr{ border-color:var(--border-1); }

.stButton > button, .stDownloadButton > button, [data-testid="stFormSubmitButton"] > button{
  border-radius:10px; border:1px solid var(--border-2); font-weight:600;
  touch-action:manipulation; -webkit-tap-highlight-color:transparent;
  transition:border-color .18s var(--ease), background .18s var(--ease), color .18s var(--ease); }
.stButton > button:hover:not(:disabled), .stDownloadButton > button:hover:not(:disabled){
  border-color:var(--cyan); color:var(--cyan); }
.stButton > button:disabled, [data-testid="stFormSubmitButton"] > button:disabled{ opacity:.42; }
[data-testid="stFormSubmitButton"] > button[kind="primary"],
.stButton > button[kind="primary"]{ background:var(--cyan); border-color:var(--cyan); color:#05070C; }
[data-testid="stFormSubmitButton"] > button[kind="primary"]:hover,
.stButton > button[kind="primary"]:hover:not(:disabled){ background:#5CC9F5; border-color:#5CC9F5; color:#05070C; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# --------------------------------------------------------------------------
# 会话状态
# --------------------------------------------------------------------------
LOG_KEY = "_log"
RUN_KEY = "_run"
QUEUE_KEY = "_queue"
TRACES_KEY = "_traces"
PROGRESS_KEY = "_progress"
HISTORY_KEY = "_history_batch"
PROMPT_SYNC_PREFIX = "_prompt_sync_"
MAX_LOG = 600
MAX_TRACES = 200

PIPELINE_STEPS = {
    "full": [("榜单捕获", ["capture"]), ("供给探测", ["probe"])],
    "capture": [("榜单捕获", ["capture"])],
    "probe": [("供给探测", ["probe"])],
    "filter": [("AI 筛选", ["filter"])],
}

STEP_RETRY_ATTEMPTS = 2
STEP_RETRY_BASE_WAIT = 3.0
LATEST_SENTINEL = "__latest__"


def _init_session():
    st.session_state.setdefault(LOG_KEY, [])
    st.session_state.setdefault(
        RUN_KEY,
        {"status": "idle", "label": "", "cmd": "", "start": None, "end": None,
         "returncode": None, "error": None, "batch_id": None, "key": None,
         "steps": [], "failed_step": None},
    )
    st.session_state.setdefault(QUEUE_KEY, None)
    st.session_state.setdefault(TRACES_KEY, [])
    st.session_state.setdefault(
        PROGRESS_KEY,
        {"pct": 0.0, "label": "", "step": "", "current": "", "message": "", "stage": ""},
    )


def _append_log(msg: str):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    logs = st.session_state[LOG_KEY]
    logs.append(line)
    if len(logs) > MAX_LOG:
        del logs[:-MAX_LOG]


def _append_trace(trace: dict):
    traces = st.session_state[TRACES_KEY]
    traces.append(trace)
    if len(traces) > MAX_TRACES:
        del traces[:-MAX_TRACES]


# --------------------------------------------------------------------------
# 后台执行（线程安全：只写 queue，不碰 st.*）
# --------------------------------------------------------------------------
def _run_step(q: "queue.Queue", prefix: str, args: list, batch_id: str | None):
    cmd_args = list(args) + ["--emit-events"]
    if batch_id:
        cmd_args += ["--batch-id", batch_id]
    q.put(("log", f"▶ {prefix}  (python -m radar {' '.join(cmd_args)})"))
    step_started = time.time()
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "radar", *cmd_args],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
    except Exception as exc:
        q.put(("error", f"命令执行失败，请检查环境: {exc}"))
        return None, batch_id
    try:
        for line in iter(proc.stdout.readline, ""):
            if not line:
                continue
            event = telemetry.parse_event_line(line)
            if event is None:
                q.put(("log", line.rstrip()))
                continue
            if batch_id is None and event.get("batch_id"):
                batch_id = event["batch_id"]
            q.put(("event", event))
    finally:
        try:
            proc.stdout.close()
        except Exception:
            pass
    try:
        returncode = proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        returncode = proc.wait()
    except Exception:
        returncode = proc.poll()
    elapsed = time.time() - step_started
    if returncode is None:
        q.put(("error", f"{prefix} 未能获取退出码，进程可能异常退出。"))
        return None, batch_id
    q.put(("log", f"■ {prefix} 结束（退出码 {returncode}，耗时 {elapsed:.1f}s）"))
    return returncode, batch_id


def _worker(q: "queue.Queue", label: str, steps: list, offset: int = 0,
            batch_id: str | None = None):
    """后台线程：串行执行各阶段子进程，解析 stdout 遥测事件并投递到 queue。"""
    total = offset + len(steps)
    for i, (step_label, args) in enumerate(steps, offset + 1):
        prefix = f"步骤 {i}/{total} · {step_label}" if total > 1 else step_label
        attempt = 0
        while True:
            attempt += 1
            attempt_prefix = (f"{prefix}（第 {attempt} 次尝试）"
                              if attempt > 1 else prefix)
            returncode, batch_id = _run_step(q, attempt_prefix, args, batch_id)
            if returncode is None:
                q.put(("done", -1))
                return
            if returncode == 0:
                break
            if returncode == EXIT_RETRYABLE and attempt <= STEP_RETRY_ATTEMPTS:
                wait = STEP_RETRY_BASE_WAIT * attempt
                q.put(("log", f"↻ {step_label} 可重试中断，{wait:.0f}s 后自动重试"
                              f"（{attempt}/{STEP_RETRY_ATTEMPTS}）…"))
                time.sleep(wait)
                continue
            if returncode == EXIT_RETRYABLE:
                q.put(("error", f"{step_label} 重试 {STEP_RETRY_ATTEMPTS} 次后仍中断，"
                                f"已保留历史产物。修正配置后可点「继续」从该步恢复。"))
                q.put(("interrupted", {"step_index": i - 1, "batch_id": batch_id,
                                       "label": label}))
                return
            q.put(("error", f"{step_label} 退出码 {returncode}，流程中止。"))
            q.put(("done", returncode))
            return
    q.put(("done", 0))


def _start_run(label: str, key: str, resume_from: int = 0, batch_id: str | None = None,
               all_steps: list | None = None, steps: list | None = None):
    """启动一次运行。steps 显式给出时优先（用于「刷新指定应用」等定制命令）。"""
    full_steps = all_steps or steps or PIPELINE_STEPS[key]
    run_steps = full_steps[resume_from:]
    q: "queue.Queue" = queue.Queue()
    st.session_state[QUEUE_KEY] = q
    st.session_state[LOG_KEY] = []
    st.session_state[TRACES_KEY] = []
    st.session_state[PROGRESS_KEY] = {
        "pct": 0.0, "label": "", "step": "", "current": "", "message": "", "stage": ""}
    st.session_state[RUN_KEY] = {
        "status": "running",
        "label": label,
        "cmd": "python -m radar " + " ".join(a for _, args in full_steps for a in args),
        "start": time.time(),
        "end": None,
        "returncode": None,
        "error": None,
        "batch_id": batch_id,
        "key": key,
        "steps": [s[0] for s in full_steps],
        "run_steps": full_steps,
        "failed_step": None,
    }
    verb = f"继续: {label}（从步骤 {resume_from + 1}/{len(full_steps)}）" if resume_from else f"启动: {label}"
    _append_log(verb)
    threading.Thread(target=_worker, args=(q, label, run_steps, resume_from, batch_id),
                     daemon=True).start()


def _resume_run():
    state = st.session_state[RUN_KEY]
    key = state.get("key")
    failed_step = state.get("failed_step")
    if not key or failed_step is None:
        return
    steps = state.get("run_steps") or PIPELINE_STEPS.get(key)
    _start_run(state.get("label", "继续"), key, resume_from=failed_step,
               batch_id=state.get("batch_id"), all_steps=steps)


def _handle_event(event: dict):
    etype = event.get("type")
    state = st.session_state[RUN_KEY]
    if event.get("batch_id") and not state.get("batch_id"):
        state["batch_id"] = event["batch_id"]
    if etype == "progress":
        st.session_state[PROGRESS_KEY] = {
            "pct": float(event.get("pct", 0.0)),
            "label": event.get("label", ""),
            "stage": event.get("stage", ""),
            "step": f"{event.get('index', 0)}/{event.get('total', 0)}",
            "current": event.get("current", ""),
            "message": event.get("message", ""),
        }
    elif etype == "stage":
        prog = dict(st.session_state[PROGRESS_KEY])
        if event.get("status") == "start":
            prog.update({"label": event.get("label", ""), "stage": event.get("stage", ""),
                         "step": f"0/{event.get('total', 0)}", "current": "",
                         "message": "开始…"})
        else:
            prog.update({"pct": float(event.get("pct", prog.get("pct", 0.0))),
                         "message": event.get("message", "")})
        st.session_state[PROGRESS_KEY] = prog
    elif etype == "trace":
        _append_trace(event)
    elif etype == "log":
        _append_log(event.get("message", ""))
    elif etype == "run":
        st.session_state[PROGRESS_KEY] = {
            **st.session_state[PROGRESS_KEY], "pct": 100.0,
            "message": event.get("message", ""),
        }


def _drain_queue():
    q = st.session_state.get(QUEUE_KEY)
    if q is None:
        return
    state = st.session_state[RUN_KEY]
    while True:
        try:
            kind, payload = q.get_nowait()
        except queue.Empty:
            break
        if kind == "log":
            _append_log(payload)
        elif kind == "event":
            _handle_event(payload)
        elif kind == "error":
            state["error"] = payload
            _append_log(f"✖ {payload}")
        elif kind == "interrupted":
            state["status"] = "interrupted"
            state["end"] = time.time()
            state["failed_step"] = payload.get("step_index", 0)
            if payload.get("batch_id"):
                state["batch_id"] = payload["batch_id"]
            _append_log("已中断，等待修正配置后继续。")
        elif kind == "done":
            state["returncode"] = payload
            state["end"] = time.time()
            if payload == 0 and not state.get("error"):
                state["status"] = "done"
                _append_log("完成（返回码 0）")
            else:
                state["status"] = "failed"
                _append_log(f"失败（返回码 {payload}）")
    if state["status"] in ("done", "failed", "interrupted"):
        st.session_state[QUEUE_KEY] = None


def _is_running() -> bool:
    return st.session_state.get(RUN_KEY, {}).get("status") == "running"


def _elapsed(state) -> str:
    if not state.get("start"):
        return ""
    end = state.get("end") or time.time()
    secs = int(end - state["start"])
    return f"{secs // 60}分{secs % 60:02d}秒"


def _status_html(state, live: bool = True) -> str:
    status = state.get("status", "idle")
    label = {"idle": "空闲", "running": "运行中", "done": "已完成",
             "failed": "失败", "interrupted": "已中断"}.get(status, status)
    if status == "running":
        label = f"运行中 · {state.get('label', '')}"
    live_attr = ' role="status" aria-live="polite"' if live else ""
    return (f'<span class="status {status}"{live_attr}>'
            f'<span class="dot" aria-hidden="true"></span>{html.escape(str(label))}</span>')


# --------------------------------------------------------------------------
# 数据读取（批次产物 + 数据库）
# --------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def _read_json_cached(path_str: str, mtime_ns: int, size: int):
    try:
        with open(path_str, encoding="utf-8") as f:
            return json.load(f), None
    except Exception as exc:
        name = Path(path_str).name
        return None, f"数据文件损坏，请重新扫描（{name}: {exc}）"


def _read_json(filename: str):
    path = DATA_DIR / filename
    if not path.exists():
        return None, None
    stat = path.stat()
    return _read_json_cached(str(path), stat.st_mtime_ns, stat.st_size)


def _read_run_json(batch_id: str, filename: str):
    path = RUNS_DIR / batch_id / filename
    if not path.exists():
        return None, None
    stat = path.stat()
    return _read_json_cached(str(path), stat.st_mtime_ns, stat.st_size)


def _artifact_reader(batch_id: str | None):
    return (lambda fn: _read_run_json(batch_id, fn)) if batch_id else _read_json


def _db_path(cfg: dict) -> Path:
    return database.resolve_db_path(cfg)


def _ensure_db(cfg: dict) -> Path:
    path = _db_path(cfg)
    database.init_db(path)
    return path


@st.cache_data(show_spinner=False)
def _db_stats(db_path: str, stamp: str):
    return database.get_stats(db_path)


@st.cache_data(show_spinner=False)
def _db_apps(db_path: str, stamp: str, filters_json: str):
    return database.get_apps(json.loads(filters_json), db_path=db_path)


def _db_stamp(db_path: Path) -> str:
    try:
        stat = db_path.stat()
        return f"{stat.st_mtime_ns}:{stat.st_size}"
    except OSError:
        return "missing"


def _load_apps(cfg: dict, filters: dict) -> list[dict]:
    db_path = _ensure_db(cfg)
    return _db_apps(str(db_path), _db_stamp(db_path), json.dumps(filters, sort_keys=True))


def _load_stats(cfg: dict) -> dict:
    db_path = _ensure_db(cfg)
    return _db_stats(str(db_path), _db_stamp(db_path))


# --------------------------------------------------------------------------
# 通用渲染片段
# --------------------------------------------------------------------------
def _clip(text, n):
    text = (text or "").strip()
    if not text:
        return ""
    return text if len(text) <= n else text[:n].rstrip() + "…"


def _empty_state(title, detail, cmd=None):
    cmd_html = f'<div class="cmd">{html.escape(cmd)}</div>' if cmd else ""
    st.markdown(
        f'<div class="empty"><div class="t">{html.escape(title)}</div>'
        f'<div class="d">{html.escape(detail)}</div>{cmd_html}</div>',
        unsafe_allow_html=True,
    )


BRAND_MARK = (
    '<svg viewBox="0 0 40 40" fill="none" aria-hidden="true">'
    '<circle cx="20" cy="20" r="15.5" stroke="#1E2735" stroke-width="1.5"/>'
    '<circle cx="20" cy="20" r="9" stroke="#2A3646" stroke-width="1.5"/>'
    '<path d="M20 20 L20 4.5 A15.5 15.5 0 0 1 35.5 20 Z" fill="rgba(56,189,248,.16)"/>'
    '<path d="M20 20 L20 4.5" stroke="#38BDF8" stroke-width="2" stroke-linecap="round"/>'
    '<path d="M20 20 L31 29" stroke="#6EE7B7" stroke-width="2" stroke-linecap="round"/>'
    '<circle cx="20" cy="20" r="2.6" fill="#E7ECF3"/>'
    '</svg>'
)

_TAG_CLASS = {
    "待刷新": "pill-refresh",
    "未探测": "pill",
    "生态空白（待验证）": "pill-blank",
    "已收藏": "pill-fav",
}


def _tag_html(tags: list[str]) -> str:
    return "".join(
        f'<span class="pill {_TAG_CLASS.get(t, "")}">{html.escape(t)}</span>'
        for t in tags
    )


# --------------------------------------------------------------------------
# 顶部品牌区
# --------------------------------------------------------------------------
def render_brand(state, stats: dict, rows: list):
    ready = sum(1 for r in rows if r["ok"])
    current_batch = store.latest_run_id() or "—"
    left, right = st.columns([3.2, 1])
    with left:
        st.markdown(
            '<div class="brand">'
            f'<div class="brand-mark">{BRAND_MARK}</div>'
            '<div><div class="brand-title">纯血鸿蒙机会雷达</div>'
            '<div class="brand-sub">Apple 榜单 → 鸿蒙供给事实 → 本地数据资产 → 按需筛选</div></div>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="brand-meta">'
            f'<div class="item">最近批次 <b>{html.escape(str(current_batch))}</b></div>'
            f'<div class="item">应用库 <b>{stats.get("total", 0)}</b></div>'
            f'<div class="item">已探测 <b>{stats.get("probed", 0)}</b></div>'
            f'<div class="item">产物 <b>{ready}/4 就绪</b></div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with right:
        st.markdown(
            f'<div style="text-align:right;padding-top:6px">{_status_html(state, live=False)}</div>',
            unsafe_allow_html=True,
        )
        if st.button("刷新页面", width="stretch", disabled=_is_running()):
            st.cache_data.clear()
            st.rerun()


def render_history_selector() -> str | None:
    """历史批次回溯器：返回选中的 batch_id（None 表示最新镜像）。"""
    options = []
    for item in store.list_runs():
        meta = item.get("meta", {})
        status = {"success": "成功", "failed": "失败", "running": "运行中",
                  "interrupted": "中断"}.get(meta.get("status"), "未知")
        options.append(f"{item['batch_id']} · {meta.get('stage', '?')} · {status}")
    if not options:
        return None
    labels = ["最新（data/ 顶层镜像）"] + options
    current = st.session_state.get(HISTORY_KEY, LATEST_SENTINEL)
    try:
        index = labels.index(current)
    except ValueError:
        index = 0
    choice = st.selectbox(
        "历史批次回溯", labels, index=index, key="_history_select",
        help="切换查看任意历史批次的归档产物；「最新」读取 data/ 顶层镜像。",
    )
    st.session_state[HISTORY_KEY] = choice
    if choice == labels[0]:
        return None
    return choice.split(" · ", 1)[0]


# --------------------------------------------------------------------------
# 运行状态条 + 实时进度 + 结构化日志（片段：仅在运行时轮询）
# --------------------------------------------------------------------------
def _progress_html(prog: dict) -> str:
    pct = max(0.0, min(100.0, float(prog.get("pct", 0.0))))
    label = prog.get("label") or "准备中"
    stage = prog.get("stage", "")
    step = prog.get("step", "")
    current = prog.get("current", "")
    message = prog.get("message", "")
    stage_txt = " · ".join(x for x in (stage, step) if x)
    meta_bits = [b for b in (current, message) if b]
    meta = " · ".join(meta_bits)
    meta_html = f'<div class="prog-meta">{html.escape(meta)}</div>' if meta else ""
    return (
        f'<div class="prog">'
        f'<div class="prog-head"><span class="prog-label">{html.escape(str(label))}</span>'
        f'<span class="prog-stage">{html.escape(stage_txt)}</span>'
        f'<span class="prog-pct">{pct:.0f}%</span></div>'
        f'<div class="prog-track" role="progressbar" aria-valuenow="{pct:.0f}" '
        f'aria-valuemin="0" aria-valuemax="100">'
        f'<div class="prog-fill" style="width:{pct:.1f}%"></div></div>'
        f'{meta_html}</div>'
    )


def _trace_card(trace: dict) -> str:
    ok = trace.get("status") == "success"
    st_cls, st_txt = ("ok", "成功") if ok else ("err", "失败")
    stage = html.escape(str(trace.get("stage") or "—"))
    target = html.escape(str(trace.get("target_name") or "—"))
    dur = trace.get("duration_s", 0)
    err = trace.get("error")
    sys_p = html.escape(str(trace.get("system_prompt") or ""))
    usr_p = html.escape(str(trace.get("user_prompt") or ""))
    raw = trace.get("raw_response")
    raw_txt = html.escape(str(raw)) if raw is not None else "（无响应）"
    body = (
        f'<div class="trace-body">'
        f'<div class="trace-sec"><div class="k">System Prompt</div>'
        f'<div class="trace-pre">{sys_p or "（空）"}</div></div>'
        f'<div class="trace-sec"><div class="k">User Prompt</div>'
        f'<div class="trace-pre">{usr_p or "（空）"}</div></div>'
        f'<div class="trace-sec"><div class="k">Raw Response</div>'
        f'<div class="trace-pre">{raw_txt}</div></div>'
        + (f'<div class="trace-sec"><div class="k">Error</div>'
           f'<div class="trace-pre">{html.escape(str(err))}</div></div>' if err else "")
        + '</div>'
    )
    return (
        f'<details class="trace"><summary class="trace-head">'
        f'<span class="trace-stage">{stage}</span>'
        f'<span class="trace-target">{target}</span>'
        f'<span class="trace-st {st_cls}">{st_txt}</span>'
        f'<span class="trace-dur">{float(dur):.2f}s</span>'
        f'</summary>{body}</details>'
    )


@st.fragment(run_every=1.0)
def _live_fragment():
    _drain_queue()
    state = st.session_state[RUN_KEY]
    if state["status"] != "running":
        st.rerun(scope="app")
        return
    prog = st.session_state[PROGRESS_KEY]
    logs = st.session_state[LOG_KEY]
    tail = "\n".join(logs[-10:]) if logs else "等待输出…"
    st.markdown(
        f'<div class="panel"><div style="display:flex;justify-content:space-between;'
        f'align-items:center;gap:12px;flex-wrap:wrap">'
        f'<div>{_status_html(state)}</div>'
        f'<div class="muted">{html.escape(state.get("cmd", ""))} · 已运行 {_elapsed(state)}</div>'
        f'</div>{_progress_html(prog)}'
        f'<div class="logbox small" role="log" aria-live="polite" '
        f'style="margin-top:10px">{html.escape(tail)}</div></div>',
        unsafe_allow_html=True,
    )


def _render_resume_controls(state):
    key = state.get("key")
    failed_step = state.get("failed_step") or 0
    steps = state.get("run_steps") or PIPELINE_STEPS.get(key, [])
    step_label = steps[failed_step][0] if failed_step < len(steps) else "未知步骤"
    st.markdown(
        f'<div class="muted" style="margin-top:8px">'
        f'将在同一批次 <b>{html.escape(str(state.get("batch_id") or "—"))}</b> 中'
        f'从「{html.escape(step_label)}」重新执行。请先到「流水线」修正配置。'
        f'</div>',
        unsafe_allow_html=True,
    )
    if st.button("继续执行（从失败步骤恢复）", type="primary"):
        _resume_run()
        st.rerun()


def render_run_strip():
    state = st.session_state[RUN_KEY]
    if state["status"] == "running":
        _live_fragment()
        return
    if state["status"] in ("done", "failed", "interrupted"):
        prog = st.session_state[PROGRESS_KEY]
        traces = st.session_state[TRACES_KEY]
        batch_id = state.get("batch_id")
        badge = f' · 批次 <b>{html.escape(str(batch_id))}</b>' if batch_id else ""
        border = {"failed": "rgba(255,138,128,.45)",
                  "interrupted": "rgba(233,165,104,.45)"}.get(
                      state["status"], "var(--border-1)")
        err_html = ""
        if state.get("error"):
            err_html = (f'<div class="muted" style="color:var(--danger);margin-top:8px">'
                        f'{html.escape(state["error"])}</div>')
        st.markdown(
            f'<div class="panel" style="border-color:{border}">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'gap:12px;flex-wrap:wrap">'
            f'<div>{_status_html(state, live=False)}</div>'
            f'<div class="muted">{html.escape(state.get("label", ""))} · 耗时 {_elapsed(state)}'
            f'{badge}</div></div>'
            f'{_progress_html(prog)}{err_html}</div>',
            unsafe_allow_html=True,
        )
        if state["status"] == "interrupted":
            _render_resume_controls(state)
        if traces:
            with st.expander(f"LLM 调用追踪（{len(traces)} 次，点击展开详情）", expanded=False):
                st.markdown("".join(_trace_card(t) for t in traces),
                            unsafe_allow_html=True)


# --------------------------------------------------------------------------
# 侧栏
# --------------------------------------------------------------------------
def sidebar_panel(cfg, stats: dict):
    st.sidebar.markdown(
        '<div class="brand-title" style="font-size:1.05rem">纯血鸿蒙机会雷达</div>'
        '<div class="brand-sub" style="font-size:.72rem">M5 · 数据资产 + 按需筛选</div>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown(_status_html(st.session_state[RUN_KEY], live=False),
                        unsafe_allow_html=True)
    st.sidebar.markdown("---")

    disabled = _is_running()
    if st.sidebar.button("全量扫描（捕获 → 探测）", type="primary",
                         disabled=disabled, width="stretch"):
        _start_run("全量扫描", "full")
        st.rerun()
    c1, c2 = st.sidebar.columns(2)
    if c1.button("仅捕获", disabled=disabled, width="stretch"):
        _start_run("仅捕获", "capture")
        st.rerun()
    if c2.button("仅探测", disabled=disabled, width="stretch"):
        _start_run("仅探测", "probe")
        st.rerun()
    if st.sidebar.button("AI 筛选（按需）", disabled=disabled, width="stretch"):
        _start_run("AI 筛选", "filter")
        st.rerun()
    if st.sidebar.button("刷新待更新应用", disabled=disabled or not stats.get("stale"),
                         width="stretch"):
        _start_run("刷新待更新", "probe", steps=[("供给探测", ["probe", "--only-stale"])])
        st.rerun()

    if st.session_state[RUN_KEY].get("status") == "interrupted":
        if st.sidebar.button("继续上次中断的流程", type="primary",
                             disabled=disabled, width="stretch", key="side_resume"):
            _resume_run()
            st.rerun()

    st.sidebar.markdown("---")
    logs = st.session_state[LOG_KEY]
    st.sidebar.markdown("**最近日志**")
    st.sidebar.markdown(
        f'<div class="logbox small">{html.escape(logs[-1] if logs else "暂无日志")}</div>',
        unsafe_allow_html=True,
    )
    st.sidebar.caption("完整日志见「流水线」标签。")


# --------------------------------------------------------------------------
# 标签 1：数据库
# --------------------------------------------------------------------------
def _filter_controls() -> dict:
    """筛选器：返回 database.get_apps 可用的 filters dict。"""
    c1, c2, c3 = st.columns([1.1, 1.2, 1.2])
    period = c1.selectbox("时间范围", ["全部", "最近 7 天", "最近 30 天", "最近 90 天"],
                          index=0)
    comp = c2.selectbox("竞品数", ["全部", "0 个（空白）", "1-2 个（低竞争）",
                                   "3+ 个（高竞争）"], index=0)
    rating = c3.selectbox("竞品评分", ["全部", "无竞品", "低分 (<3.5)",
                                       "中分 (3.5-4.0)", "高分 (>4.0)"], index=0)

    c4, c5, c6 = st.columns([1.2, 1.1, 1.4])
    pain = c4.selectbox("差评痛点", ["全部", "无差评痛点", "有高频痛点", "有中频痛点"],
                        index=0)
    refresh = c5.selectbox("刷新状态", ["全部", "仅待刷新"], index=0)
    query = c6.text_input("搜索", placeholder="应用名 / JTBD / 开发者")

    c7, c8 = st.columns([1, 3])
    favorite = c7.checkbox("仅看收藏", value=False)

    filters = {}
    days_map = {"最近 7 天": 7, "最近 30 天": 30, "最近 90 天": 90}
    if period in days_map:
        filters["days"] = days_map[period]
    comp_map = {"0 个（空白）": "0", "1-2 个（低竞争）": "1-2",
                "3+ 个（高竞争）": "3+"}
    if comp in comp_map:
        filters["competitor"] = comp_map[comp]
    rating_map = {"无竞品": "none", "低分 (<3.5)": "low",
                  "中分 (3.5-4.0)": "mid", "高分 (>4.0)": "high"}
    if rating in rating_map:
        filters["rating"] = rating_map[rating]
    pain_map = {"无差评痛点": "none", "有高频痛点": "high", "有中频痛点": "mid"}
    if pain in pain_map:
        filters["pain"] = pain_map[pain]
    if refresh == "仅待刷新":
        filters["refresh"] = "stale"
    if favorite:
        filters["favorite"] = True
    if (query or "").strip():
        filters["query"] = query.strip()
    return filters


def _apps_table(apps: list[dict]) -> list[dict]:
    """转成表格行（中文表头）。"""
    rows = []
    for app in apps:
        row = viewmodel.app_row(app)
        row["状态"] = " / ".join(row["tags"]) or "—"
        rows.append(row)
    return rows


def render_database(cfg: dict, stats: dict):
    cards = viewmodel.stats_cards(stats)
    cells = "".join(
        f'<div class="stat"><div class="stat-k">{html.escape(c["label"])}</div>'
        f'<div class="stat-v {c["tone"]}">{html.escape(c["value"])}</div>'
        f'<div class="stat-s">{html.escape(c["sub"])}</div></div>'
        for c in cards
    )
    st.markdown(f'<div class="statline">{cells}</div>', unsafe_allow_html=True)
    st.markdown("")

    if not stats.get("total"):
        _empty_state("数据库还是空的", "先运行一次「全量扫描」把 Apple 榜单应用抓进来。",
                     "python -m radar capture")
        return

    with st.expander("筛选器", expanded=True):
        filters = _filter_controls()

    apps = _load_apps(cfg, filters)
    st.markdown(
        f'<div class="muted">当前条件：{html.escape(viewmodel.filter_echo(filters))}'
        f' · 命中 {len(apps)} 个应用</div>',
        unsafe_allow_html=True,
    )
    if not apps:
        _empty_state("没有符合条件的应用", "放宽筛选条件，或先运行一次探测补充供给事实。")
        return

    rows = _apps_table(apps)
    table = [{
        "应用名": r["name"],
        "JTBD": _clip(r["jtbd"], 42),
        "首次出现": r["first_seen"],
        "最近出现": r["last_seen"],
        "竞品数": r["competitor_count"],
        "平均评分": r["avg_rating"],
        "痛点": r["pain"],
        "状态": r["状态"],
    } for r in rows]
    st.dataframe(table, width="stretch", hide_index=True, height=420)

    st.markdown("---")
    c1, c2, c3 = st.columns([2.2, 1.2, 1.2])
    labels = {r["apple_id"]: f'{r["name"]} · 竞品 {r["competitor_count"]} · {r["pain"]}'
              for r in rows}
    selected_id = c1.selectbox("查看详情 / 操作", list(labels.keys()),
                               format_func=lambda k: labels[k], key="db_detail_select")
    disabled = _is_running()
    if c2.button("刷新选中应用", disabled=disabled, width="stretch"):
        _start_run("刷新选中", "probe",
                   steps=[("供给探测", ["probe", "--apple-ids", selected_id])])
        st.rerun()
    csv_bytes = database.csv_text(filters, db_path=_db_path(cfg))
    c3.download_button("导出 CSV", data=csv_bytes.encode("utf-8-sig"),
                       file_name="harmony_apps.csv", mime="text/csv",
                       width="stretch",
                       help=f"按当前筛选条件导出 {len(apps)} 条")

    if stats.get("stale"):
        if st.button(f"刷新全部待刷新（{stats['stale']} 个）", disabled=disabled):
            _start_run("刷新待更新", "probe",
                       steps=[("供给探测", ["probe", "--only-stale"])])
            st.rerun()

    if selected_id:
        render_app_detail(cfg, selected_id)


def render_app_detail(cfg: dict, apple_id: str):
    detail = database.get_app_detail(apple_id, db_path=_db_path(cfg))
    if not detail:
        return
    tags = viewmodel.status_tags(detail)
    with st.container(key="detail_card"):
        st.markdown(
            f'<div class="detail-head"><span class="detail-name">'
            f'{html.escape(str(detail.get("name", "?")))}</span>'
            f'{_tag_html(tags)}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="muted">by {html.escape(str(detail.get("artist") or "—"))}'
            f' · Apple ID {html.escape(str(detail.get("apple_id") or "—"))}</div>',
            unsafe_allow_html=True,
        )
        if detail.get("jtbd"):
            st.markdown(f'<div class="muted">JTBD · {html.escape(str(detail["jtbd"]))}</div>',
                        unsafe_allow_html=True)
        if detail.get("pick_reason"):
            st.markdown(f'<div class="muted">入选理由 · '
                        f'{html.escape(str(detail["pick_reason"]))}</div>',
                        unsafe_allow_html=True)

        rounds = detail.get("rounds") or []
        comps = detail.get("competitors") or []
        ratings = [c.get("rating") for c in comps
                   if isinstance(c.get("rating"), (int, float))]
        avg = f"{sum(ratings) / len(ratings):.2f}" if ratings else "—"
        high = sum(1 for c in comps for p in c.get("pain_points", [])
                   if p.get("frequency") == "高频")
        mid = sum(1 for c in comps for p in c.get("pain_points", [])
                  if p.get("frequency") == "中频")
        st.markdown(
            '<div class="facts">'
            f'<div class="fact"><div class="fact-k">最近探测</div>'
            f'<div class="fact-v" style="font-size:.9rem">'
            f'{html.escape(viewmodel.fmt_ts(detail.get("last_probe_at") or ""))}</div></div>'
            f'<div class="fact"><div class="fact-k">Probe 状态</div>'
            f'<div class="fact-v" style="font-size:.9rem">'
            f'{html.escape(str(detail.get("probe_status") or "未探测"))}</div></div>'
            f'<div class="fact"><div class="fact-k">竞品数</div>'
            f'<div class="fact-v">{detail.get("competitor_count", 0)}</div></div>'
            f'<div class="fact"><div class="fact-k">平均评分</div>'
            f'<div class="fact-v">{avg}</div></div>'
            f'<div class="fact"><div class="fact-k">高频 / 中频痛点</div>'
            f'<div class="fact-v" style="color:var(--amber)">{high} / {mid}</div></div>'
            f'<div class="fact"><div class="fact-k">采集轮次</div>'
            f'<div class="fact-v">{len(rounds)}</div></div>'
            '</div>',
            unsafe_allow_html=True,
        )

        if detail.get("is_blank"):
            st.markdown(
                '<div class="verdict" style="margin-top:14px;border-color:rgba(255,213,79,.35)">'
                '本次检索未发现有效同类竞品 —— 这是待验证假设，'
                '不等于该品类在市场绝对不存在。</div>',
                unsafe_allow_html=True,
            )

        if comps:
            st.markdown('<div class="panel-title" style="margin-top:16px">竞品与差评痛点</div>',
                        unsafe_allow_html=True)
            for c in comps[:6]:
                pains = c.get("pain_points") or []
                pain_html = "".join(
                    f'<div class="pain">· [{html.escape(str(p.get("frequency") or "—"))}] '
                    f'{html.escape(str(p.get("issue") or ""))}'
                    + (f' <span class="muted">「{html.escape(_clip(p.get("quote"), 60))}」</span>'
                       if p.get("quote") else "")
                    + '</div>'
                    for p in pains
                )
                st.markdown(
                    f'<div class="comp-row"><div class="comp-name">'
                    f'{html.escape(str(c.get("name") or "?"))}</div>'
                    f'<div class="comp-meta">'
                    f'<span>评分 {html.escape(str(c.get("rating") if c.get("rating") is not None else "—"))}</span>'
                    f'<span>下载 {html.escape(str(c.get("downloads") or "—"))}</span>'
                    f'<span>分类 {html.escape(str(c.get("category") or "—"))}</span>'
                    f'<span>采集于 {html.escape(viewmodel.fmt_ts(c.get("probed_at") or ""))}</span></div>'
                    f'{pain_html or "<div class=muted>本次未提炼到痛点</div>"}</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.markdown('<div class="muted" style="margin-top:12px">尚无竞品快照。</div>',
                        unsafe_allow_html=True)

        if len(rounds) > 1:
            with st.expander(f"Probe 历史（{len(rounds)} 轮）", expanded=False):
                timeline = [{
                    "探测时间": viewmodel.fmt_ts(r.get("probed_at", "")),
                    "竞品数": r.get("competitor_count", 0),
                    "平均评分": r.get("avg_rating") if r.get("avg_rating") is not None else "—",
                    "高频痛点": r.get("high_pain_count", 0),
                } for r in rounds]
                st.dataframe(timeline, width="stretch", hide_index=True)

        c1, c2 = st.columns([1, 3])
        fav_label = "取消收藏" if detail.get("is_favorite") else "加入收藏"
        if c1.button(fav_label, key=f"fav_{apple_id}"):
            database.toggle_favorite(apple_id, db_path=_db_path(cfg))
            st.cache_data.clear()
            st.rerun()


# --------------------------------------------------------------------------
# 标签 2：AI 助手
# ------------------------------------------------------------------
FILTER_PROMPT_NAME = "user_filter"


def _filter_results(cfg: dict) -> dict:
    data = store.load_filter_results()
    return data if isinstance(data, dict) else {}


def render_filter_prompt_editor():
    """AI 助手页的专用编辑器（widget key 与「提示词微调」页区分开）。"""
    name = FILTER_PROMPT_NAME
    spec = prompts.PROMPT_TEMPLATES[name]
    current = prompts.get_prompt(name)
    customized = prompts.is_customized(name)
    badge = ('<span class="custom-badge">已自定义</span>' if customized else
             '<span class="muted">内建默认</span>')
    ph = "".join(f'<span class="ph-chip">{{{p}}}</span>' for p in spec["placeholders"])
    st.markdown(
        f'<div class="prompt-head"><span class="prompt-name">{html.escape(spec["label"])}</span>'
        f'<span class="muted">prompts/{name}.txt</span>{badge}</div>'
        f'<div style="margin-bottom:6px">必要占位符: {ph}</div>',
        unsafe_allow_html=True,
    )
    widget_key = f"filter_edit_{name}"
    sync_key = f"{PROMPT_SYNC_PREFIX}filter_{name}"
    if st.session_state.get(sync_key) != current:
        st.session_state[widget_key] = current
        st.session_state[sync_key] = current
    st.session_state.setdefault(widget_key, current)
    text = st.text_area("筛选提示词", key=widget_key, height=380,
                        label_visibility="collapsed")
    c1, c2, c3 = st.columns([1, 1, 2])
    if c1.button("保存提示词", type="primary", width="stretch",
                 disabled=_is_running()):
        errors = prompts.validate_prompt(name, text)
        if errors:
            for e in errors:
                st.error(e)
        else:
            prompts.save_prompt(name, text)
            st.session_state[sync_key] = text
            # 提示词微调页的编辑器需要重新同步
            st.session_state.pop(f"{PROMPT_SYNC_PREFIX}{name}", None)
            st.toast("筛选提示词已保存，下次筛选立即生效")
            st.rerun()
    if c2.button("恢复默认", width="stretch", disabled=_is_running()):
        prompts.reset_prompt(name)
        st.session_state.pop(sync_key, None)
        st.session_state.pop(f"{PROMPT_SYNC_PREFIX}{name}", None)
        st.toast("已恢复默认提示词")
        st.rerun()
    if c3.button("导出全部默认模板", width="stretch", disabled=_is_running()):
        written = prompts.export_defaults(overwrite=False)
        st.toast(f"已导出 {len(written)} 个默认模板（不覆盖已有自定义文件）")


def _render_opportunity_card(opp: dict, cfg: dict, idx: int):
    blank = bool(opp.get("is_blank"))
    key_prefix = "gap_card_" if blank else "opp_card_"
    with st.container(key=f"{key_prefix}{idx}"):
        tags = ['<span class="pill pill-blank">生态空白（待验证）</span>'] if blank else []
        apple_id = str(opp.get("apple_id") or "")
        detail = database.get_app_detail(apple_id, db_path=_db_path(cfg)) if apple_id else {}
        if detail and detail.get("is_favorite"):
            tags.append('<span class="pill pill-fav">已收藏</span>')
        st.markdown(
            f'<div class="detail-head"><span class="detail-name">'
            f'{html.escape(str(opp.get("name", "?")))}</span>{"".join(tags)}'
            f'<span class="muted">by {html.escape(str(opp.get("artist") or "—"))}</span></div>'
            f'<div class="muted">JTBD · {html.escape(str(opp.get("jtbd") or "—"))}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(f'<div class="verdict" style="margin-top:10px">'
                    f'{html.escape(str(opp.get("why_pick") or ""))}</div>',
                    unsafe_allow_html=True)
        st.markdown(
            '<div class="facts" style="margin-top:10px">'
            f'<div class="fact"><div class="fact-k">竞品概况</div>'
            f'<div class="fact-v" style="font-size:.85rem;font-weight:500">'
            f'{html.escape(str(opp.get("competitors_summary") or "—"))}</div></div>'
            f'<div class="fact"><div class="fact-k">突破切入点</div>'
            f'<div class="fact-v" style="font-size:.85rem;font-weight:500">'
            f'{html.escape(str(opp.get("attack_vector") or "—"))}</div></div>'
            '</div>',
            unsafe_allow_html=True,
        )
        feats = opp.get("native_features") or []
        if feats:
            st.markdown("**鸿蒙原生能力** · " + "、".join(
                f'<span class="pill pill-fav">{html.escape(str(f))}</span>' for f in feats),
                unsafe_allow_html=True)
        c1, c2 = st.columns([1, 4])
        if apple_id:
            fav_label = "取消收藏" if (detail or {}).get("is_favorite") else "加入收藏"
            if c1.button(fav_label, key=f"opp_fav_{apple_id}_{idx}"):
                database.toggle_favorite(apple_id, db_path=_db_path(cfg))
                st.cache_data.clear()
                st.rerun()


def render_filter_page(cfg: dict, stats: dict):
    st.markdown(
        '<div class="muted">把数据库里的全部事实交给 AI，按你写的标准挑机会。'
        'AI 只负责筛选与说明理由，不做固定打分，也不会丢弃任何原始数据。</div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="panel-title" style="margin-top:12px">第一步 · 筛选提示词</div>',
                unsafe_allow_html=True)
    render_filter_prompt_editor()

    st.markdown('<div class="panel-title" style="margin-top:12px">第二步 · 执行筛选</div>',
                unsafe_allow_html=True)
    filter_cfg = cfg.get("filter") or {}
    st.markdown(
        f'<div class="muted">取样范围：最近 {filter_cfg.get("recent_days", 60)} 天出现、'
        f'最多 {filter_cfg.get("max_apps", 300)} 个应用；输出上限 '
        f'{filter_cfg.get("max_results", 20)} 个机会。可在「流水线 → 配置」调整。</div>',
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns([1, 3])
    if c1.button("执行 AI 筛选", type="primary", width="stretch",
                 disabled=_is_running()):
        _start_run("AI 筛选", "filter")
        st.rerun()
    if not stats.get("probed"):
        c2.caption("提示：还没有任何 probe 结果，AI 只能基于 Apple 侧信息判断，"
                   "建议先跑一次「仅探测」。")

    st.markdown('<div class="panel-title" style="margin-top:16px">第三步 · 结果</div>',
                unsafe_allow_html=True)
    result = _filter_results(cfg)
    if not result:
        _empty_state("还没有筛选结果", "写好提示词后点「执行 AI 筛选」。",
                     "python -m radar filter")
        return

    ops = result.get("opportunities", [])
    st.markdown(
        f'<div class="muted">生成时间 {html.escape(viewmodel.fmt_ts(result.get("filtered_at", "")))}'
        f' · 模型 {html.escape(str(result.get("model") or "—"))}'
        f' · 输入 {result.get("input_count", 0)} 个应用'
        f' · 输出 {len(ops)} 个机会</div>',
        unsafe_allow_html=True,
    )
    if not ops:
        _empty_state("AI 没有挑出机会", "放宽提示词里的筛选标准，或补充更多应用后再试。")
        return

    from radar.pipeline.filter import render_markdown
    markdown = render_markdown(result)
    c1, c2 = st.columns([1, 3])
    c1.download_button("导出 Markdown", data=markdown,
                       file_name="harmony_opportunities.md", mime="text/markdown",
                       width="stretch")
    c2.caption(f"共 {len(ops)} 个候选，点击卡片下方按钮可加入本地收藏。")

    for i, opp in enumerate(ops):
        _render_opportunity_card(opp, cfg, i)


# --------------------------------------------------------------------------
# 标签 3：流水线
# --------------------------------------------------------------------------
def _config_from_form(form):
    return {
        "llm": {
            "base_url": form["base_url"].strip(),
            "api_key": form["api_key"],
            "model": form["model"].strip(),
            "timeout": int(form["timeout"]),
        },
        "capture": {
            "regions": form["regions"],
            "charts": form["charts"],
            "feed_limit": int(form["feed_limit"]),
            "max_benchmarks": int(form["max_benchmarks"]),
            "llm_input_limit": int(form["llm_input_limit"]),
            "rss_retries": int(form["rss_retries"]),
        },
        "probe": {
            "max_comments_per_app": int(form["max_comments_per_app"]),
            "bad_rating_threshold": int(form["bad_rating_threshold"]),
            "max_competitors": int(form["max_competitors"]),
            "max_keywords_per_benchmark": int(form["max_keywords_per_benchmark"]),
            "comment_pages": int(form["comment_pages"]),
            "filter_candidates_limit": int(form["filter_candidates_limit"]),
            "enable_llm_filter": bool(form["enable_llm_filter"]),
        },
        "database": {
            "path": form["db_path"].strip(),
            "refresh_threshold_days": int(form["refresh_threshold_days"]),
            "auto_clean_days": int(form["auto_clean_days"]),
        },
        "filter": {
            "recent_days": int(form["recent_days"]),
            "max_apps": int(form["max_apps"]),
            "max_results": int(form["max_results"]),
        },
    }


def _render_config_form(cfg):
    llm = cfg.get("llm", {})
    cap = cfg.get("capture", {})
    prb = cfg.get("probe", {})
    dbc = cfg.get("database", {})
    flt = cfg.get("filter", {})

    with st.form("config_form"):
        st.markdown("**LLM 推理配置**")
        st.caption("影响 capture / probe / filter 全部阶段。")
        base_url = st.text_input("API 地址", value=llm.get("base_url", ""))
        api_key = st.text_input("API Key", value=llm.get("api_key", ""), type="password")
        model = st.text_input("模型名称", value=llm.get("model", ""))
        timeout = st.number_input("超时（秒）", min_value=10, max_value=1800,
                                  value=int(llm.get("timeout", 120)), step=10)

        st.markdown("---")
        st.markdown("**榜单捕获配置**")
        st.caption("影响 capture 阶段。")
        regions = st.multiselect("地区", ["cn", "us"],
                                 default=cap.get("regions", ["cn", "us"]))
        charts = st.multiselect("榜单", ["top-free", "top-paid"],
                                default=cap.get("charts", ["top-free", "top-paid"]))
        feed_limit = st.number_input("RSS 榜单条数", min_value=1, max_value=200,
                                     value=int(cap.get("feed_limit", 100)), step=10)
        max_benchmarks = st.number_input("精选标杆上限", min_value=1, max_value=100,
                                         value=int(cap.get("max_benchmarks", 20)), step=1)
        llm_input_limit = st.number_input("LLM 输入上限", min_value=1, max_value=500,
                                          value=int(cap.get("llm_input_limit", 200)), step=10)
        rss_retries = st.number_input("RSS 拉取重试次数", min_value=1, max_value=10,
                                      value=int(cap.get("rss_retries", 3)), step=1)

        st.markdown("---")
        st.markdown("**华为探测配置**")
        st.caption("影响 probe 阶段。")
        max_comments_per_app = st.number_input("单应用评价上限", min_value=1, max_value=500,
                                               value=int(prb.get("max_comments_per_app", 50)), step=10)
        bad_rating_threshold = st.number_input("差评星级阈值", min_value=1, max_value=5,
                                               value=int(prb.get("bad_rating_threshold", 3)), step=1)
        max_competitors = st.number_input("单标杆最大竞品数", min_value=1, max_value=20,
                                          value=int(prb.get("max_competitors", 3)), step=1)
        max_keywords_per_benchmark = st.number_input(
            "单标杆最大关键词数", min_value=1, max_value=8,
            value=int(prb.get("max_keywords_per_benchmark", 4)), step=1)
        comment_pages = st.number_input("评论抓取页数", min_value=1, max_value=20,
                                        value=int(prb.get("comment_pages", 4)), step=1)
        filter_candidates_limit = st.number_input("LLM 候选过滤上限", min_value=1, max_value=50,
                                                  value=int(prb.get("filter_candidates_limit", 8)), step=1)
        enable_llm_filter = st.checkbox("启用 LLM 语义去噪",
                                        value=bool(prb.get("enable_llm_filter", True)))

        st.markdown("---")
        st.markdown("**本地数据库配置**")
        st.caption("数据库只增不减；超过阈值未 probe 的应用会被标记为待刷新。")
        db_path = st.text_input("数据库路径", value=str(dbc.get("path", "data/apps.db")))
        refresh_threshold_days = st.number_input("待刷新阈值（天）", min_value=1,
                                                 max_value=3650,
                                                 value=int(dbc.get("refresh_threshold_days", 30)),
                                                 step=1)
        auto_clean_days = st.number_input("自动清理（天，0=禁用）", min_value=0,
                                          max_value=3650,
                                          value=int(dbc.get("auto_clean_days", 0)), step=10)

        st.markdown("---")
        st.markdown("**AI 筛选配置**")
        st.caption("决定单次筛选喂给 AI 的数据量与返回上限。")
        recent_days = st.number_input("取样时间窗口（天）", min_value=1, max_value=3650,
                                      value=int(flt.get("recent_days", 60)), step=10)
        max_apps = st.number_input("取样应用上限", min_value=1, max_value=2000,
                                   value=int(flt.get("max_apps", 300)), step=10)
        max_results = st.number_input("返回机会上限", min_value=1, max_value=100,
                                      value=int(flt.get("max_results", 20)), step=1)

        c1, c2 = st.columns(2)
        submitted = c1.form_submit_button("保存配置", width="stretch")
        reset = c2.form_submit_button("重置为默认值", width="stretch")

    if submitted:
        new_cfg = _config_from_form({
            "base_url": base_url, "api_key": api_key, "model": model, "timeout": timeout,
            "regions": regions, "charts": charts, "feed_limit": feed_limit,
            "max_benchmarks": max_benchmarks, "llm_input_limit": llm_input_limit,
            "rss_retries": rss_retries,
            "max_comments_per_app": max_comments_per_app,
            "bad_rating_threshold": bad_rating_threshold, "max_competitors": max_competitors,
            "max_keywords_per_benchmark": max_keywords_per_benchmark,
            "comment_pages": comment_pages,
            "filter_candidates_limit": filter_candidates_limit,
            "enable_llm_filter": enable_llm_filter,
            "db_path": db_path, "refresh_threshold_days": refresh_threshold_days,
            "auto_clean_days": auto_clean_days,
            "recent_days": recent_days, "max_apps": max_apps, "max_results": max_results,
        })
        errors = validate_config(new_cfg)
        if errors:
            for e in errors:
                st.error(e)
        else:
            save_config(new_cfg)
            st.cache_data.clear()
            st.toast("配置已保存到 config.toml")
            st.rerun()

    if reset:
        save_config(DEFAULTS)
        st.cache_data.clear()
        st.toast("已重置为默认配置")
        st.rerun()


def _render_artifacts(batch_id: str | None = None):
    rows = viewmodel.load_artifacts(_artifact_reader(batch_id))["rows"]
    parts = ['<div class="panel" style="padding:6px 0">']
    for r in rows:
        state = ('<span class="state ok"><span class="dot" aria-hidden="true"></span>已就绪</span>'
                 if r["ok"] else
                 '<span class="state bad"><span class="dot" aria-hidden="true"></span>缺失</span>')
        if r["ok"]:
            count = r["count"] if r["count"] is not None else "—"
            meta = f'{count} 条'
            meta += f'<span style="margin-left:14px">{html.escape(viewmodel.fmt_ts(r["ts"]))}</span>'
        else:
            meta = html.escape(r["note"])
        parts.append(
            f'<div class="artifact"><div><span class="name">'
            f'{html.escape(r["title"])}</span> <span class="muted">{html.escape(r["file"])}</span></div>'
            f'<div class="meta">{state}<span style="margin-left:14px">{meta}</span></div></div>'
        )
    parts.append("</div>")
    st.markdown("".join(parts), unsafe_allow_html=True)


def _render_traces_panel(traces, batch_id):
    if batch_id and not traces:
        traces = store.load_traces(batch_id)
    if not traces:
        _empty_state("暂无 LLM 调用追踪",
                     "运行任一阶段后，每次大模型调用的 Prompt 与原始响应会在此展示。",
                     "python -m radar probe")
        return
    ok = sum(1 for t in traces if t.get("status") == "success")
    err = len(traces) - ok
    total_s = sum(float(t.get("duration_s") or 0) for t in traces)
    st.markdown(
        f'<div class="muted">共 {len(traces)} 次调用 · 成功 {ok} · 失败 {err} · '
        f'累计 {total_s:.1f}s</div>',
        unsafe_allow_html=True,
    )
    stages = sorted({str(t.get("stage") or "—") for t in traces})
    stage_filter = st.multiselect("按阶段过滤", stages, default=stages)
    shown = [t for t in traces if str(t.get("stage") or "—") in stage_filter]
    if not shown:
        st.info("当前过滤条件下无追踪记录。")
        return
    st.markdown("".join(_trace_card(t) for t in shown), unsafe_allow_html=True)


def _render_text_log_panel(logs):
    text = "\n".join(logs) if logs else "暂无日志"
    st.markdown(
        f'<div class="logbox" role="log" aria-live="polite" aria-label="执行日志">'
        f'{html.escape(text)}</div>',
        unsafe_allow_html=True,
    )
    if logs:
        c1, c2 = st.columns([1, 3])
        c1.download_button("下载日志", data="\n".join(logs), file_name="radar_run.log",
                           mime="text/plain", width="stretch")
        with c2.popover("复制日志"):
            st.code("\n".join(logs), language="text")


def render_pipeline(cfg, stats: dict):
    completeness = viewmodel.completeness(
        viewmodel.load_artifacts(_artifact_reader(None))["rows"], stats)
    st.markdown(
        f'<div class="muted">当前批次：<b>{html.escape(str(store.latest_run_id() or "—"))}</b>'
        f' &nbsp; 完整度：{"完整" if completeness.get("complete") else "不完整"}'
        f' &nbsp; 下一步：{html.escape(str(completeness.get("next_action", "—")))}</div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="panel-title" style="margin-top:12px">数据产物状态</div>',
                unsafe_allow_html=True)
    _render_artifacts()
    st.caption("所有产物均为 data/ 目录下的原子写入结果；数据库是跨批次累积的核心资产。")

    st.markdown('<div class="panel-title" style="margin-top:18px">流水线执行</div>',
                unsafe_allow_html=True)
    disabled = _is_running()
    c1, c2, c3, c4 = st.columns(4)
    if c1.button("全量扫描", disabled=disabled, width="stretch", key="c_full"):
        _start_run("全量扫描", "full")
        st.rerun()
    if c2.button("仅捕获", disabled=disabled, width="stretch", key="c_cap"):
        _start_run("仅捕获", "capture")
        st.rerun()
    if c3.button("仅探测", disabled=disabled, width="stretch", key="c_prb"):
        _start_run("仅探测", "probe")
        st.rerun()
    if c4.button("AI 筛选", disabled=disabled, width="stretch", key="c_flt"):
        _start_run("AI 筛选", "filter")
        st.rerun()
    st.caption("全量扫描按 捕获 → 探测 顺序执行；网络/LLM 临时故障会自动重试，"
               "仍失败则进入「中断」态，修正配置后可点「继续」从失败步骤恢复。")

    st.markdown('<div class="panel-title" style="margin-top:18px">执行日志</div>',
                unsafe_allow_html=True)
    logs = st.session_state[LOG_KEY]
    traces = st.session_state[TRACES_KEY]
    state = st.session_state[RUN_KEY]
    view = st.radio(
        "视图", ["结构化 Traces 检查器", "纯文本详细模式"],
        horizontal=True, label_visibility="collapsed", key="_log_view",
    )
    if view == "结构化 Traces 检查器":
        _render_traces_panel(traces, state.get("batch_id"))
    else:
        _render_text_log_panel(logs)

    st.markdown('<div class="panel-title" style="margin-top:18px">配置</div>',
                unsafe_allow_html=True)
    _render_config_form(cfg)


# --------------------------------------------------------------------------
# 标签 4：历史批次 / 提示词
# --------------------------------------------------------------------------
def render_history():
    batch_id = render_history_selector()
    if not batch_id:
        st.markdown('<div class="muted">当前展示 data/ 顶层镜像（最近一次成功批次）。</div>',
                    unsafe_allow_html=True)
        return
    st.markdown(f'<div class="muted">正在回溯历史批次 <b>{html.escape(batch_id)}</b>。</div>',
                unsafe_allow_html=True)
    _render_artifacts(batch_id)
    st.markdown('<div class="panel-title" style="margin-top:16px">LLM 调用追踪</div>',
                unsafe_allow_html=True)
    _render_traces_panel(store.load_traces(batch_id), batch_id)

    result = store.load_run_json(batch_id, "filter_results.json")
    if isinstance(result, dict) and result.get("opportunities"):
        st.markdown('<div class="panel-title" style="margin-top:16px">该批次筛选结果</div>',
                    unsafe_allow_html=True)
        st.dataframe([{
            "应用": o.get("name", ""),
            "JTBD": _clip(o.get("jtbd"), 40),
            "为什么选它": _clip(o.get("why_pick"), 80),
            "切入点": _clip(o.get("attack_vector"), 60),
        } for o in result["opportunities"]], width="stretch", hide_index=True)


def _group_templates():
    groups: dict[str, list[str]] = {}
    for name, spec in prompts.PROMPT_TEMPLATES.items():
        groups.setdefault(spec["stage"], []).append(name)
    return groups


def render_prompts():
    st.markdown(
        '<div class="muted">提示词模板独立存放于 <code>prompts/*.txt</code>，'
        '保存后下一次运行立即生效；运行批次会归档当次使用的快照。'
        '「恢复官方默认」会删除自定义文件并回退到内建模板。</div>',
        unsafe_allow_html=True,
    )
    if st.button("导出全部默认模板到 prompts/", disabled=_is_running()):
        written = prompts.export_defaults(overwrite=False)
        st.toast(f"已导出 {len(written)} 个默认模板（不覆盖已有自定义文件）")

    groups = _group_templates()
    tabs = st.tabs(list(groups.keys()))
    for tab, (stage, names) in zip(tabs, groups.items()):
        with tab:
            for name in names:
                _render_prompt_editor(name)


def _render_prompt_editor(name: str):
    spec = prompts.PROMPT_TEMPLATES[name]
    current = prompts.get_prompt(name)
    customized = prompts.is_customized(name)
    badge = ('<span class="custom-badge">已自定义</span>' if customized else
             '<span class="muted">内建默认</span>')
    ph = "".join(f'<span class="ph-chip">{{{p}}}</span>' for p in spec["placeholders"])
    st.markdown(
        f'<div class="prompt-head"><span class="prompt-name">{html.escape(spec["label"])}</span>'
        f'<span class="muted">{name}.txt</span>{badge}</div>'
        + (f'<div style="margin-bottom:6px">必要占位符: {ph}</div>' if spec["placeholders"] else ""),
        unsafe_allow_html=True,
    )
    widget_key = f"prompt_edit_{name}"
    sync_key = f"{PROMPT_SYNC_PREFIX}{name}"
    if st.session_state.get(sync_key) != current:
        st.session_state[widget_key] = current
        st.session_state[sync_key] = current
    st.session_state.setdefault(widget_key, current)
    text = st.text_area("模板内容", key=widget_key, height=200,
                        label_visibility="collapsed")
    c1, c2 = st.columns([1, 1])
    if c1.button("保存模板", key=f"save_{name}", width="stretch",
                 disabled=_is_running()):
        errors = prompts.validate_prompt(name, text)
        if errors:
            for e in errors:
                st.error(e)
        else:
            prompts.save_prompt(name, text)
            st.session_state[sync_key] = text
            st.toast(f"{spec['label']} 已保存")
            st.rerun()
    if c2.button("恢复官方默认", key=f"reset_{name}", width="stretch",
                 disabled=_is_running()):
        prompts.reset_prompt(name)
        st.session_state.pop(sync_key, None)
        st.toast(f"{spec['label']} 已恢复默认")
        st.rerun()
    st.markdown("---")


# --------------------------------------------------------------------------
# 主入口
# --------------------------------------------------------------------------
def main():
    _init_session()
    _drain_queue()

    cfg = load_config()
    stats = _load_stats(cfg)
    sidebar_panel(cfg, stats)

    rows = viewmodel.load_artifacts(_artifact_reader(None))["rows"]
    render_brand(st.session_state[RUN_KEY], stats, rows)
    render_run_strip()
    st.markdown("---")

    t1, t2, t3, t4, t5 = st.tabs(
        ["数据库", "AI 助手", "流水线", "历史批次", "提示词微调"])

    with t1:
        render_database(cfg, stats)
    with t2:
        render_filter_page(cfg, stats)
    with t3:
        render_pipeline(cfg, stats)
    with t4:
        render_history()
    with t5:
        render_prompts()

    st.sidebar.markdown("---")
    st.sidebar.caption("Harmony Opportunity Radar · M5 数据资产 + 按需筛选")


if __name__ == "__main__":
    main()
