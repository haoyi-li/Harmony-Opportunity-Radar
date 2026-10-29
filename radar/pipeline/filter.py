# -*- coding: utf-8 -*-
"""AI 事后筛选：把本地数据库全量事实交给 LLM，按用户提示词挑机会。

设计要点:
- 不做预先固定打分，不设分数门槛，不丢弃任何原始数据；
- 输入是累积的数据库（含尚未 probe 的应用），输出是「候选机会」，
  由用户自己决定要不要做；
- 提示词完全外部化（prompts/user_filter_system.txt / user_filter.txt），
  用户可随时改写筛选标准。

熔断保护：LLM 全挂或产出为空时抛 PipelineAbortError，绝不写空产物覆盖历史。
"""
import json

from radar import database, store, telemetry
from radar.config import load_config
from radar.llm import LLMClient, LLMError
from radar.pipeline import PipelineAbortError, RetryableAbortError
from radar.prompts import USER_FILTER_SYSTEM, USER_FILTER_USER

STAGE = "filter"


def run(cfg: dict | None = None) -> int:
    """执行一次 AI 筛选，返回入选机会数。"""
    if cfg is None:
        cfg = load_config()

    filter_cfg = cfg.get("filter", {}) or {}
    recent_days = int(filter_cfg.get("recent_days", 60))
    max_apps = int(filter_cfg.get("max_apps", 300))
    max_results = int(filter_cfg.get("max_results", 20))

    db_path = database.resolve_db_path(cfg)
    database.init_db(db_path)
    apps = database.apps_for_filter(recent_days, db_path=db_path, limit=max_apps)
    if not apps:
        raise PipelineAbortError(
            "数据库中最近 %d 天内没有可筛选的应用，请先运行 capture / probe。"
            % recent_days)

    print(f"[AI filter] 载入 {len(apps)} 个应用（最近 {recent_days} 天，上限 {max_apps}）")
    tel = telemetry.current()
    if tel is not None:
        tel.stage_start(STAGE, len(apps))

    payload = json.dumps(_slim_apps(apps), ensure_ascii=False, indent=2)
    user_prompt = USER_FILTER_USER.format(database_json=payload,
                                          max_results=max_results)
    llm = LLMClient(cfg["llm"], stage=STAGE, target="机会筛选")
    try:
        raw = llm.chat_json(USER_FILTER_SYSTEM, user_prompt,
                            temperature=0.1, stage=STAGE, target="机会筛选")
    except LLMError as e:
        raise RetryableAbortError(f"AI 筛选调用失败: {e}") from e

    opportunities = _normalize_results(raw, apps, max_results)
    if not opportunities:
        raise PipelineAbortError(
            "AI 筛选未返回任何有效机会（或全部格式非法），已保留历史筛选结果。")

    saved_at = store._now()
    store.save_filter_results({
        "filtered_at": saved_at,
        "model": cfg.get("llm", {}).get("model", ""),
        "input_count": len(apps),
        "recent_days": recent_days,
        "max_results": max_results,
        "count": len(opportunities),
        "opportunities": opportunities,
    })
    print(f"\n=== filter 完成: {len(opportunities)} 个候选机会 → filter_results.json ===")
    if tel is not None:
        tel.stage_done(STAGE, f"{len(opportunities)} 个候选机会")
    return len(opportunities)


def _slim_apps(apps: list[dict]) -> list[dict]:
    """压缩成 LLM 需要的字段，避免把整库 JSON 塞进上下文。"""
    slim = []
    for app in apps:
        slim.append({
            "name": app.get("name", ""),
            "apple_id": app.get("apple_id", ""),
            "artist": app.get("artist", ""),
            "jtbd": app.get("jtbd", ""),
            "pick_reason": app.get("pick_reason", ""),
            "source_charts": app.get("source_charts", ""),
            "first_seen": app.get("first_seen", ""),
            "last_seen": app.get("last_seen", ""),
            "last_probe_at": app.get("last_probe_at") or "",
            "probe_status": app.get("probe_status") or "",
            "is_blank": bool(app.get("is_blank")),
            "competitor_count": app.get("competitor_count", 0),
            "competitors": app.get("competitors", []),
        })
    return slim


def _normalize_results(raw, apps: list[dict], max_results: int) -> list[dict]:
    """校验并标准化 LLM 返回；剔除结构非法项，补齐缺失字段。"""
    items = raw
    if isinstance(raw, dict):
        items = next((v for v in raw.values() if isinstance(v, list)), None)
    if not isinstance(items, list):
        return []

    known = {str(a.get("apple_id", "")): a for a in apps if a.get("apple_id")}
    by_name = {str(a.get("name", "")).strip(): a for a in apps if a.get("name")}
    results = []
    for item in items:
        if not isinstance(item, dict):
            continue
        apple_id = str(item.get("apple_id", "") or "").strip()
        name = str(item.get("name", "") or "").strip()
        if not name:
            continue
        # apple_id 缺失时按名称回填，避免 AI 漏写 ID 后丢失竞品事实
        source = known.get(apple_id) or by_name.get(name) or {}
        if not apple_id:
            apple_id = str(source.get("apple_id", "") or "")
        features = item.get("native_features")
        if not isinstance(features, list):
            features = []
        results.append({
            "name": name,
            "apple_id": apple_id,
            "jtbd": str(item.get("jtbd") or source.get("jtbd", "")),
            "why_pick": str(item.get("why_pick", "")),
            "attack_vector": str(item.get("attack_vector", "")),
            "native_features": [str(f) for f in features if str(f).strip()],
            "competitors_summary": str(item.get("competitors_summary", "")),
            "artist": source.get("artist", ""),
            "is_blank": bool(source.get("is_blank")),
            "competitor_count": source.get("competitor_count", 0),
        })
        if len(results) >= max_results:
            break
    return results


def render_markdown(result: dict) -> str:
    """把筛选结果渲染为可导出的 Markdown。"""
    ops = result.get("opportunities", [])
    lines = [
        "# 鸿蒙机会雷达 · AI 筛选结果",
        "",
        f"> 生成时间: {result.get('filtered_at', '')}  |  "
        f"模型: {result.get('model', '?')}  |  "
        f"输入应用: {result.get('input_count', 0)}  |  输出机会: {len(ops)}",
        "",
    ]
    for rank, o in enumerate(ops, 1):
        blank = " 🟦 生态空白（待验证）" if o.get("is_blank") else ""
        lines.append(f"## {rank}. {o.get('name', '?')}{blank}")
        lines.append("")
        lines.append(f"- JTBD: {o.get('jtbd', '')}")
        lines.append(f"- 为什么选它: {o.get('why_pick', '')}")
        lines.append(f"- 竞品概况: {o.get('competitors_summary', '')}")
        lines.append(f"- 突破切入点: {o.get('attack_vector', '')}")
        feats = o.get("native_features") or []
        if feats:
            lines.append("- 鸿蒙原生能力: " + "、".join(feats))
        lines.append("")
    return "\n".join(lines)
