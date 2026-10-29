# -*- coding: utf-8 -*-
"""M5 数据库读模型：把 SQLite 查询结果转成界面直接可渲染的展示层。

设计约束:
- 只读数据库与 data/ 产物，不写任何文件；
- 不依赖 Streamlit，便于离线单元测试；
- 派生字段（竞品数、平均评分、痛点强度、状态标签、时间差）全部在此计算；
- 数据库缺失或为空时返回可操作的空态信息，不伪造数据。
"""
import json
from datetime import datetime

# 读取的批次产物清单：文件名、中文标题、时间戳键、列表计数键
ARTIFACT_SPECS = [
    ("raw_feed.json", "原始榜单", "captured_at", "apps"),
    ("benchmarks.json", "精选标杆", "captured_at", "benchmarks"),
    ("probe.json", "鸿蒙供给探测", "probed_at", "benchmarks"),
    ("filter_results.json", "AI 筛选结果", "filtered_at", "opportunities"),
]

# 竞品数量分档
COMP_NONE = "0 个（空白）"
COMP_LOW = "1-2 个（低竞争）"
COMP_HIGH = "3+ 个（高竞争）"


def distinct_pain_count(pain_list) -> int:
    """统计去重后的不同痛点数量（差评原文常含完全重复文本）。"""
    seen = set()
    for p in pain_list or []:
        text = str(p or "").strip()
        if text:
            seen.add(text)
    return len(seen)


def fmt_ts(ts: str) -> str:
    """ISO 时间戳 → 本地可读格式；解析失败原样回退，不丢信息。"""
    if not ts:
        return "—"
    raw = str(ts)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return raw.replace("T", " ").replace("+08:00", "")


def days_since(ts: str) -> int | None:
    """距今天数；无法解析时返回 None（界面显示「未知」而非 0）。"""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None
    now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
    return max(0, (now - dt).days)


def comp_bucket(count: int) -> str:
    if not count:
        return COMP_NONE
    if count <= 2:
        return COMP_LOW
    return COMP_HIGH


def rating_label(avg) -> str:
    if avg is None:
        return "—"
    return f"{float(avg):.1f}"


def pain_label(app: dict) -> str:
    high = int(app.get("high_pain_count", 0) or 0)
    mid = int(app.get("mid_pain_count", 0) or 0)
    if high:
        return f"高频 {high}"
    if mid:
        return f"中频 {mid}"
    return "—"


def status_tags(app: dict) -> list[str]:
    """应用状态标签：生态空白（待验证）/ 待刷新 / 未 probe / 已收藏。"""
    tags = []
    if app.get("needs_refresh"):
        tags.append("待刷新")
    if not app.get("last_probe_at"):
        tags.append("未探测")
    elif app.get("is_blank"):
        tags.append("生态空白（待验证）")
    if app.get("is_favorite"):
        tags.append("已收藏")
    return tags


def app_row(app: dict) -> dict:
    """数据库行 → 列表展示行。"""
    return {
        "name": app.get("name", ""),
        "apple_id": app.get("apple_id", ""),
        "jtbd": app.get("jtbd", ""),
        "first_seen": fmt_ts(app.get("first_seen", "")),
        "last_seen": fmt_ts(app.get("last_seen", "")),
        "first_seen_days": days_since(app.get("first_seen", "")),
        "last_seen_days": days_since(app.get("last_seen", "")),
        "competitor_count": int(app.get("comp_total", app.get("competitor_count", 0)) or 0),
        "competitor_bucket": comp_bucket(
            int(app.get("comp_total", app.get("competitor_count", 0)) or 0)),
        "avg_rating": rating_label(app.get("comp_avg_rating")),
        "pain": pain_label(app),
        "high_pain_count": int(app.get("high_pain_count", 0) or 0),
        "mid_pain_count": int(app.get("mid_pain_count", 0) or 0),
        "probe_status": app.get("probe_status", ""),
        "tags": status_tags(app),
        "is_blank": bool(app.get("is_blank")),
        "needs_refresh": bool(app.get("needs_refresh")),
        "is_favorite": bool(app.get("is_favorite")),
    }


def stats_cards(stats: dict) -> list[dict]:
    """概览卡片数据（label / value / sub / tone）。"""
    return [
        {"label": "应用总数", "value": str(stats.get("total", 0)),
         "sub": "数据库累计", "tone": ""},
        {"label": "最近 7 天新增", "value": str(stats.get("new_7", 0)),
         "sub": "新进榜单", "tone": "sig"},
        {"label": "最近 30 天新增", "value": str(stats.get("new_30", 0)),
         "sub": "观察窗口", "tone": ""},
        {"label": "待刷新", "value": str(stats.get("stale", 0)),
         "sub": f"超过 {stats.get('refresh_days', 30)} 天未 probe", "tone": ""},
        {"label": "确认空白", "value": str(stats.get("blank", 0)),
         "sub": "待验证假设", "tone": "gap"},
        {"label": "有竞品", "value": str(stats.get("with_competitors", 0)),
         "sub": "已找到同类", "tone": ""},
        {"label": "痛点应用", "value": str(stats.get("pain_apps", 0)),
         "sub": "含高频/中频差评", "tone": ""},
        {"label": "收藏", "value": str(stats.get("favorites", 0)),
         "sub": "本地标记", "tone": ""},
    ]


def filter_echo(filters: dict) -> str:
    """把筛选条件渲染成一句人话，避免用户忘记当前过滤。"""
    parts = []
    days = filters.get("days")
    if days not in (None, "", "all"):
        parts.append(f"最近 {days} 天出现")
    if filters.get("query"):
        parts.append(f"搜索「{filters['query']}」")
    if filters.get("competitor"):
        parts.append({"0": "无竞品", "1-2": "1-2 个竞品", "3+": "3+ 个竞品"}
                     .get(filters["competitor"], ""))
    if filters.get("rating"):
        parts.append({"none": "无评分", "low": "低分(<3.5)",
                      "mid": "中分(3.5-4.0)", "high": "高分(>4.0)"}
                     .get(filters["rating"], ""))
    if filters.get("pain"):
        parts.append({"none": "无差评痛点", "high": "有高频痛点",
                      "mid": "有中频痛点"}.get(filters["pain"], ""))
    if filters.get("refresh") == "stale":
        parts.append("仅待刷新")
    if filters.get("favorite"):
        parts.append("仅收藏")
    parts = [p for p in parts if p]
    return " · ".join(parts) if parts else "全部应用"


def load_artifacts(reader) -> dict:
    """按 ARTIFACT_SPECS 加载批次产物。

    reader: 可调用对象 reader(filename) -> (data, error)。
    返回 {"files": {...}, "errors": [...], "rows": [...]}。
    """
    files = {}
    errors = []
    rows = []
    for filename, title, ts_key, list_key in ARTIFACT_SPECS:
        data, err = reader(filename)
        files[filename] = data
        if err:
            errors.append(err)
            rows.append({"file": filename, "title": title, "ok": False,
                         "count": None, "ts": "", "note": "文件损坏"})
            continue
        if data is None:
            rows.append({"file": filename, "title": title, "ok": False,
                         "count": None, "ts": "", "note": "未生成"})
            continue
        count, ts = _artifact_summary(data, ts_key, list_key)
        rows.append({"file": filename, "title": title, "ok": True,
                     "count": count, "ts": ts, "note": ""})
    return {"files": files, "errors": errors, "rows": rows}


def _artifact_summary(data, ts_key, list_key):
    if isinstance(data, dict):
        lst = data.get(list_key)
        count = data.get("count")
        if count is None and isinstance(lst, list):
            count = len(lst)
        ts = data.get(ts_key, "") or ""
        return count, ts
    if isinstance(data, list):
        return len(data), ""
    return None, ""


def completeness(rows, db_stats: dict | None = None) -> dict:
    """判断当前数据完整度与下一步动作。"""
    ok = {r["file"]: r["ok"] for r in rows}
    total = int((db_stats or {}).get("total", 0) or 0)
    probed = int((db_stats or {}).get("probed", 0) or 0)

    if total and probed:
        return {"stage": "probe", "complete": True,
                "next_action": "数据库已积累供给事实，可按需运行 AI 筛选。"}
    if total:
        return {"stage": "capture", "complete": False,
                "next_action": "已有应用入库，请运行「仅探测」抓取鸿蒙竞品与差评。"}
    if ok.get("benchmarks.json"):
        return {"stage": "capture", "complete": False,
                "next_action": "已有精选标杆，请运行「仅探测」抓取鸿蒙竞品。"}
    if ok.get("raw_feed.json"):
        return {"stage": "raw", "complete": False,
                "next_action": "仅完成榜单抓取，请运行「全量扫描」。"}
    return {"stage": "none", "complete": False,
            "next_action": "数据库为空，请先运行「全量扫描」。"}


def parse_search_keywords(raw) -> list[str]:
    """兼容数据库返回的 JSON 字符串与已解析列表。"""
    if isinstance(raw, list):
        return [str(x) for x in raw]
    try:
        data = json.loads(raw or "[]")
        return [str(x) for x in data] if isinstance(data, list) else []
    except (ValueError, TypeError):
        return []


