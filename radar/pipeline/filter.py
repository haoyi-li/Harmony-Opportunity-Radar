# -*- coding: utf-8 -*-
"""AI 增量筛选：只对「没有有效裁决」的应用做逐应用 accept/reject 判定。

设计要点:
- 双库分层：读 apps.db 的原始事实，写 decisions.db 的裁决产物；
- 增量缓存：已裁决且依据未失效的应用直接跳过，第二轮不再调用 LLM；
- 失效规则：从未裁决 / catalog_only 裁决后已 probe / TTL 过期 / 可选哈希变化；
- 逐应用裁决：模型必须返回输入中的每个应用（accept 或 reject）并给出理由，
  reject 也入库，避免下一轮重复分析；
- 分批调用：单批默认 50 个应用，避免一次塞入数百个应用导致超时或截断；
- 筛选结果仍写 data/filter_results.json（累计最新 accept），供 WebUI 与导出使用；
- 熔断保护：LLM 全挂或一条有效裁决都没有时保留历史产物，绝不写空覆盖。
"""
import hashlib
import json
from datetime import datetime, timedelta, timezone

from radar import database, store, telemetry
from radar.config import load_config
from radar.llm import LLMClient, LLMError
from radar.pipeline import PipelineAbortError, RetryableAbortError
from radar.prompts import USER_FILTER_SYSTEM, USER_FILTER_USER

STAGE = "filter"


def run(cfg: dict | None = None) -> int:
    """执行一次增量 AI 筛选，返回本轮 accept 数量。"""
    if cfg is None:
        cfg = load_config()

    filter_cfg = cfg.get("filter", {}) or {}
    max_apps = int(filter_cfg.get("max_apps", 300))
    batch_size = max(1, int(filter_cfg.get("batch_size", 50)))
    ttl_days = max(1, int(filter_cfg.get("verdict_ttl_days", 90)))
    enable_hash = bool(filter_cfg.get("enable_criteria_hash", False))

    db_path = database.resolve_db_path(cfg)
    decisions_path = database.resolve_decisions_db_path(cfg)
    database.init_db(db_path)
    database.init_decisions_db(decisions_path)

    criteria_hash = _criteria_hash(cfg) if enable_hash else ""
    apps = database.apps_needing_verdict(
        db_path=db_path,
        decisions_db_path=decisions_path,
        limit=max_apps,
        criteria_hash=criteria_hash,
        include_hash_change=enable_hash,
    )
    if not apps:
        print("[filter] 没有待裁决应用，全部命中缓存。")
        return 0

    total_apps = database.count_apps(db_path)
    cached_count = max(0, int(total_apps) - len(apps))
    print(f"[filter] {len(apps)} 个应用需要裁决"
          f"（库内共 {total_apps} 个，已缓存 {cached_count} 个）")

    telemetry.progress(STAGE, 0, message=f"待裁决 {len(apps)} 个应用")
    tel = telemetry.current()
    if tel is not None:
        tel.stage_start(STAGE, len(apps))

    batch = store.active_batch()
    batch_id = batch.batch_id if batch is not None else ""
    model = cfg.get("llm", {}).get("model", "")
    filtered_at = _now()
    run_id = database.start_filter_run(
        batch_id, model, criteria_hash, len(apps), cached_count,
        decisions_db_path=decisions_path)

    llm = LLMClient(cfg["llm"], stage=STAGE, target="机会筛选")
    all_verdicts: list[dict] = []
    accepts = rejects = 0
    try:
        for start in range(0, len(apps), batch_size):
            chunk = apps[start:start + batch_size]
            label = f"{start + 1}-{start + len(chunk)}/{len(apps)}"
            payload = json.dumps(_slim_apps(chunk), ensure_ascii=False, indent=2)
            system_prompt = USER_FILTER_SYSTEM.format(historical_context="")
            user_prompt = USER_FILTER_USER.format(
                database_json=payload, max_results="")
            try:
                raw = llm.chat_json(system_prompt, user_prompt, temperature=0.1,
                                    stage=STAGE, target=f"机会筛选 {label}")
            except LLMError as e:
                _save_filter_results(cfg, decisions_path, db_path,
                                     len(apps), accepts, rejects, model,
                                     filtered_at, cached_count, batch_id)
                raise RetryableAbortError(
                    f"AI 筛选调用失败（{label}）: {e}\n"
                    f"已写入本轮已完成的 {len(all_verdicts)} 条裁决，"
                    f"下次运行会从剩余应用继续。") from e

            verdicts = _normalize_verdicts(raw, chunk)
            if not verdicts:
                print(f"      [warn] 批次 {label} 未返回有效裁决，跳过")
                continue
            database.insert_verdicts(
                _verdict_rows(verdicts, model, batch_id, criteria_hash,
                              filtered_at, ttl_days),
                decisions_db_path=decisions_path)
            all_verdicts.extend(verdicts)
            accepts += sum(1 for v in verdicts if v["verdict"] == "accept")
            rejects += sum(1 for v in verdicts if v["verdict"] == "reject")
            telemetry.progress(STAGE, start + len(chunk),
                               message=f"已裁决 {start + len(chunk)}/{len(apps)}")

        if not all_verdicts:
            raise PipelineAbortError(
                "AI 筛选未返回任何有效裁决（格式非法或遗漏全部应用），"
                "已保留历史筛选结果。")
    finally:
        database.finish_filter_run(run_id, accepts, rejects,
                                   decisions_db_path=decisions_path)

    _save_filter_results(cfg, decisions_path, db_path, len(apps),
                         accepts, rejects, model, filtered_at,
                         cached_count, batch_id)
    print(f"\n=== filter 完成: 本轮 {accepts} 个入选 / {rejects} 个拒绝 "
          f"→ decisions.db + filter_results.json ===")
    if tel is not None:
        tel.stage_done(STAGE, f"{accepts} 个入选 / {rejects} 个拒绝")
    return accepts


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _calc_expires(filtered_at: str, days: int) -> str:
    try:
        base = datetime.fromisoformat(filtered_at)
    except (TypeError, ValueError):
        base = datetime.now(timezone.utc).astimezone()
    return (base + timedelta(days=days)).isoformat(timespec="seconds")


def _criteria_hash(cfg: dict) -> str:
    """提示词 + 模型哈希（仅在 enable_criteria_hash=true 时用于失效判断）。"""
    content = (USER_FILTER_SYSTEM + USER_FILTER_USER
               + str(cfg.get("llm", {}).get("model", "")))
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


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
            "genre": app.get("genre", ""),
            "avg_rating": app.get("avg_rating"),
            "rating_count": app.get("rating_count"),
            "price": app.get("price"),
            "best_rank": app.get("best_rank"),
            "chart_hits": app.get("chart_hits") or [],
            "description": str(app.get("description") or "")[:240],
            "is_benchmark": bool(app.get("is_benchmark")),
            "first_seen": app.get("first_seen", ""),
            "last_seen": app.get("last_seen", ""),
            "last_probe_at": app.get("last_probe_at") or "",
            "probe_status": app.get("probe_status") or "",
            "is_blank": bool(app.get("is_blank")),
            "competitor_count": app.get("competitor_count", 0),
            "competitors": app.get("competitors", []),
        })
    return slim


def _normalize_verdicts(raw, apps: list[dict]) -> list[dict]:
    """校验并标准化逐应用裁决；剔除无法定位或字段非法的项。"""
    items = raw
    if isinstance(raw, dict):
        items = next((v for v in raw.values() if isinstance(v, list)), None)
    if not isinstance(items, list):
        return []

    known = {str(a.get("apple_id", "")): a for a in apps if a.get("apple_id")}
    by_name = {str(a.get("name", "")).strip(): a for a in apps if a.get("name")}
    results: list[dict] = []
    seen_ids: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        apple_id = str(item.get("apple_id", "") or "").strip()
        name = str(item.get("name", "") or "").strip()
        source = known.get(apple_id) or by_name.get(name)
        if source is None:
            continue
        if not apple_id:
            apple_id = str(source.get("apple_id", "") or "")
        if not apple_id or apple_id in seen_ids:
            continue
        verdict = str(item.get("verdict", "") or "").strip().lower()
        if verdict not in ("accept", "reject"):
            continue
        seen_ids.add(apple_id)

        reason = str(item.get("reason", "") or "").strip()
        if not reason:
            reason = ("模型未给出理由（需人工复核）" if verdict == "accept"
                      else "未说明理由")
        features = item.get("native_features")
        if not isinstance(features, list):
            features = []
        score = None
        if verdict == "accept":
            raw_score = item.get("score")
            try:
                score = max(0.0, min(100.0, float(str(raw_score))))
            except (TypeError, ValueError):
                score = None
        results.append({
            "apple_id": apple_id,
            "name": name or source.get("name", ""),
            "verdict": verdict,
            "score": score,
            "reason": reason,
            "attack_vector": str(item.get("attack_vector", "") or ""),
            "native_features": [str(f).strip() for f in features if str(f).strip()],
            "competitors_summary": str(item.get("competitors_summary", "") or ""),
            "source": source,
        })
    return results


def _verdict_rows(verdicts: list[dict], model: str, batch_id: str,
                  criteria_hash: str, filtered_at: str,
                  ttl_days: int) -> list[dict]:
    expires_at = _calc_expires(filtered_at, ttl_days)
    rows = []
    for v in verdicts:
        app = v.get("source") or {}
        rows.append({
            "apple_id": v["apple_id"],
            "verdict": v["verdict"],
            "score": v.get("score"),
            "reason": v.get("reason", ""),
            "attack_vector": v.get("attack_vector", ""),
            "native_features": json.dumps(v.get("native_features") or [],
                                          ensure_ascii=False),
            "competitors_summary": v.get("competitors_summary", ""),
            "basis": "probed" if app.get("last_probe_at") else "catalog_only",
            "app_metrics_version": app.get("metrics_version"),
            "probe_snapshot_at": app.get("last_probe_at"),
            "competitor_count_snapshot": app.get("competitor_count", 0),
            "criteria_hash": criteria_hash,
            "model": model,
            "batch_id": batch_id,
            "filtered_at": filtered_at,
            "expires_at": expires_at,
        })
    return rows


def _save_filter_results(cfg: dict, decisions_path, db_path, input_count: int,
                         accepts: int, rejects: int, model: str,
                         filtered_at: str, cached_count: int,
                         batch_id: str) -> None:
    """把「每个应用最新一条 accept 裁决」写为累计结果（供 WebUI / 导出）。"""
    try:
        rows = database.latest_accepted_verdicts(
            decisions_db_path=decisions_path, db_path=db_path)
    except Exception as e:
        print(f"      [warn] 读取累计裁决失败，保留历史筛选结果: {e}")
        return

    opportunities = []
    for row in rows:
        try:
            features = json.loads(row.get("native_features") or "[]")
        except (TypeError, ValueError):
            features = []
        opportunities.append({
            "name": row.get("app_name") or "",
            "apple_id": row.get("apple_id") or "",
            "artist": row.get("app_artist") or "",
            "jtbd": row.get("app_jtbd") or "",
            "why_pick": row.get("reason") or "",
            "score": row.get("score"),
            "attack_vector": row.get("attack_vector") or "",
            "native_features": features if isinstance(features, list) else [],
            "competitors_summary": row.get("competitors_summary") or "",
            "is_blank": bool(row.get("is_blank")),
            "competitor_count": row.get("competitor_count", 0),
            "genre": row.get("genre") or "",
            "avg_rating": row.get("avg_rating"),
            "best_rank": row.get("best_rank"),
            "filtered_at": row.get("filtered_at") or "",
            "basis": row.get("basis") or "",
        })

    store.save_filter_results({
        "filtered_at": filtered_at,
        "model": model,
        "batch_id": batch_id,
        "input_count": input_count,
        "cached_count": cached_count,
        "verdict_count": accepts + rejects,
        "run_accept_count": accepts,
        "reject_count": rejects,
        "count": len(opportunities),
        "opportunities": opportunities,
    })


def render_markdown(result: dict) -> str:
    """把筛选结果渲染为可导出的 Markdown。"""
    ops = result.get("opportunities", [])
    lines = [
        "# 鸿蒙机会雷达 · AI 筛选结果",
        "",
        f"> 生成时间: {result.get('filtered_at', '')}  |  "
        f"模型: {result.get('model', '?')}  |  "
        f"本轮输入应用: {result.get('input_count', 0)}  |  "
        f"累计机会: {len(ops)}",
        "",
    ]
    for rank, o in enumerate(ops, 1):
        blank = " 🟦 生态空白（待验证）" if o.get("is_blank") else ""
        score = f"（{o['score']:.0f} 分）" if isinstance(o.get("score"), (int, float)) else ""
        lines.append(f"## {rank}. {o.get('name', '?')}{score}{blank}")
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
