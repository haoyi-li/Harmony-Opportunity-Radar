# -*- coding: utf-8 -*-
"""M1 标杆捕获流水线: RSS 拉取 → iTunes 补详情 → 全量目录入库 → LLM 精选。

产物（都在 data/ 下）:
  raw_feed.json    原始榜单全量快照（LLM 处理前，便于复盘/重跑）
  benchmarks.json  精选后的标杆列表（M2 probe 的输入）

数据库分两层写入：所有 RSS 应用（is_benchmark=0）先进入 apps.db 目录层，
LLM 精选出的标杆再标记 is_benchmark=1；指标变化时追加 app_snapshots。
"""
import json
import time
from datetime import datetime, timezone

import httpx

from radar import database, store, telemetry
from radar.llm import LLMClient, LLMError
from radar.pipeline import PipelineAbortError, RetryableAbortError
from radar.prompts import CAPTURE_SYSTEM, CAPTURE_USER
from radar.sources import apple_rss

STAGE = "capture"
ITUNES_LOOKUP_URL = "https://itunes.apple.com/lookup"


def run(cfg: dict) -> int:
    """执行捕获流水线，返回标杆数量（用于 CLI 汇总输出）。"""
    cap_cfg = cfg["capture"]

    # ---- 第 1 步: 拉取 Apple 榜单原始数据 ----
    print("[1/3] 拉取 Apple RSS 榜单 "
          f"({','.join(cap_cfg['regions'])} × {','.join(cap_cfg['charts'])}) ...")
    tel = telemetry.current()
    if tel is not None:
        tel.stage_start(STAGE, 1, "榜单捕获")
    telemetry.progress(STAGE, 0, message="拉取 Apple RSS 榜单…")
    apps = apple_rss.fetch_all(
        regions=cap_cfg["regions"],
        charts=cap_cfg["charts"],
        limit=cap_cfg["feed_limit"],
        retries=cap_cfg.get("rss_retries"),
    )
    if not apps:
        # 榜单全挂通常是网络/上游临时故障，归为可重试中断
        raise RetryableAbortError("所有榜单均拉取失败，检查网络后重试")

    # ---- 第 1.5 步: iTunes lookup 补齐目录层详情（无 LLM 成本）----
    if cap_cfg.get("enrich_itunes", True):
        telemetry.progress(STAGE, 0, message="iTunes 补全应用详情…")
        apps = _enrich_with_itunes(apps)
        print(f"      iTunes 详情补全完成（{len(apps)} 个应用）")

    # 原始快照先落盘——即使后续 LLM 失败，也不用重新拉网络
    llm_limit = int(cap_cfg.get("llm_input_limit", 200))
    source_dist: dict[str, int] = {}
    for a in apps:
        src = a.get("source", "")
        source_dist[src] = source_dist.get(src, 0) + 1
    truncated = len(apps) > llm_limit
    store.save_raw_feed({
        "captured_at": _now(),
        "count": len(apps),
        "raw_count": len(apps),
        "llm_input_count": min(len(apps), llm_limit),
        "truncated": truncated,
        "source_distribution": source_dist,
        "apps": apps,
    })
    print(f"      共 {len(apps)} 个应用（去重后），已存 raw_feed.json"
          + (f"，其中前 {llm_limit} 个送入 LLM（已截断）" if truncated else ""))
    telemetry.progress(STAGE, 0, message=f"已拉取 {len(apps)} 个应用")

    # ---- 第 1.8 步: 全量目录先入库（即使 LLM 挂掉，目录资产仍保留）----
    catalog_stats, catalog_error = _persist_catalog(cfg, apps, _now())
    if catalog_error:
        print(f"      [warn] 目录层入库失败：{catalog_error}（JSON 产物仍有效）")
    else:
        print(f"      目录层入库 {catalog_stats['catalog']} 个应用，"
              f"新增快照 {catalog_stats['snapshots']} 条")

    # ---- 第 2 步: LLM 精选 + JTBD 抽象 ----
    print(f"[2/3] LLM 精选中（模型: {cfg['llm']['model']}）...")
    telemetry.progress(STAGE, 0, message="LLM 精选中…")
    llm = LLMClient(cfg["llm"], stage=STAGE, target="榜单精选")
    # 只喂 LLM 需要的精简字段，减小上下文；上限可配置，默认 200 以免推理模型超时
    slim_apps = [
        {"name": a["name"], "artist": a["artist"], "apple_id": a["apple_id"],
         "rank": a["rank"]}
        for a in apps
    ][:llm_limit]
    prompt_user = CAPTURE_USER.format(
        apps_json=json.dumps(slim_apps, ensure_ascii=False),
        max_benchmarks=cap_cfg["max_benchmarks"],
    )
    try:
        picks = llm.chat_json(CAPTURE_SYSTEM, prompt_user, stage=STAGE, target="榜单精选")
    except LLMError as e:
        # LLM 挂了不让整条流水线白跑：原始数据已在盘上，给用户明确指引
        raise RetryableAbortError(
            f"LLM 精选失败: {e}\n"
            f"原始数据已保存到 raw_feed.json，全量目录已写入 apps.db；"
            f"修好 LLM 服务（{cfg['llm']['base_url']}）后重跑 capture 即可，无需重新拉取。"
        ) from e

    if not isinstance(picks, list):
        raise PipelineAbortError(f"LLM 返回了非数组结构: {type(picks)}")
    if not picks:
        raise PipelineAbortError("LLM 精选结果为空，触发熔断：保留历史 benchmarks.json 不被覆盖。")

    # ---- 第 3 步: 回填原始榜单元数据（rank/url/source）并落盘 ----
    # apple_id 为主键，精确规范化名称为回退；两者都不命中则跳过并告警
    by_id, by_name = _build_identity_index(apps)
    benchmarks = []
    invalid_selections = []
    for p in picks:
        origin, match = _resolve_identity(p, by_id, by_name)
        if origin is None:
            invalid_selections.append(p.get("name", ""))
            print(f"      [warn] LLM 选择无法在原始榜单中定位，跳过: {p.get('name', '')}")
            continue
        benchmarks.append(_build_benchmark(p, origin, match))

    if not benchmarks:
        raise PipelineAbortError(
            "LLM 选择均无法与原始榜单匹配，触发熔断：保留历史 benchmarks.json 不被覆盖。")

    captured_at = _now()
    store.save_benchmarks({
        "captured_at": captured_at,
        "model": cfg["llm"]["model"],
        "count": len(benchmarks),
        "capture_stats": {
            "raw_count": len(apps),
            "llm_input_count": len(slim_apps),
            "truncated": truncated,
            "invalid_selections": invalid_selections,
            "source_distribution": source_dist,
        },
        "benchmarks": benchmarks,
    })

    # ---- 标记 LLM 精选标杆（目录层已在前面入库，不重复计数）----
    marked, db_error = _persist_benchmarks(cfg, benchmarks, captured_at)
    print(f"[3/3] 完成！精选 {len(benchmarks)} 个标杆，已存 benchmarks.json")
    if db_error:
        print(f"      [warn] 标杆标记失败：{db_error}（JSON 产物仍有效）")
    else:
        print(f"      标杆标记 {marked} 个（目录层 {catalog_stats['catalog']} 个，"
              f"快照 {catalog_stats['snapshots']} 条）")
    if tel is not None:
        tel.stage_done(STAGE, f"精选 {len(benchmarks)} 个标杆")
    return len(benchmarks)


def _enrich_with_itunes(apps: list[dict], batch_size: int = 100,
                        sleep_s: float = 0.5) -> list[dict]:
    """批量调用 iTunes lookup 补齐 genre/description/评分等目录层详情。

    失败时保留 RSS 字段静默降级：capture 的主目标是拿到应用清单，
    详情缺失不应让整轮抓取失败。
    """
    targets = [a for a in apps if str(a.get("apple_id", "") or "").strip()]
    if not targets:
        return apps

    for start in range(0, len(targets), batch_size):
        batch = targets[start:start + batch_size]
        ids = [str(a.get("apple_id")).strip() for a in batch]
        results: dict[str, dict] = {}
        for attempt in range(1, 3):
            try:
                with httpx.Client(trust_env=False) as client:
                    resp = client.get(
                        ITUNES_LOOKUP_URL,
                        params={"id": ",".join(ids), "country": "cn"},
                        timeout=20,
                        follow_redirects=True,
                    )
                resp.raise_for_status()
                results = {str(r.get("trackId", "")): r
                           for r in resp.json().get("results", [])
                           if r.get("trackId") is not None}
                break
            except (httpx.HTTPError, KeyError, ValueError) as e:
                if attempt < 2:
                    time.sleep(min(1.5 ** attempt, 5))
                else:
                    print(f"      [warn] iTunes 补详情失败（第 {start // batch_size + 1} 批）: {e}")
        for app in batch:
            itunes = results.get(str(app.get("apple_id", "")).strip())
            if not itunes:
                continue
            app["genre"] = itunes.get("primaryGenreName", "") or ""
            app["description"] = itunes.get("description", "") or ""
            app["avg_rating"] = itunes.get("averageUserRating")
            app["rating_count"] = itunes.get("userRatingCount")
            app["price"] = itunes.get("price")
            app["release_date"] = itunes.get("releaseDate", "") or ""
        if start + batch_size < len(targets):
            time.sleep(sleep_s)
    return apps


def _persist_catalog(cfg: dict, apps: list[dict],
                     seen_at: str) -> tuple[dict, str]:
    """把全量目录写入 apps.db，返回（统计, 错误信息）。"""
    stats = {"catalog": 0, "snapshots": 0}
    try:
        db_path = database.resolve_db_path(cfg)
        database.init_db(db_path)
        result = database.upsert_catalog_apps(apps, db_path=db_path, seen_at=seen_at)
        stats = {"catalog": result["apps"], "snapshots": result["snapshots"]}
        database.mark_needs_refresh(
            int((cfg.get("database") or {}).get("refresh_threshold_days", 30)),
            db_path=db_path)
        auto_clean = int((cfg.get("database") or {}).get("auto_clean_days", 0) or 0)
        if auto_clean > 0:
            database.clean_old_apps(auto_clean, db_path=db_path)
        return stats, ""
    except Exception as e:
        return stats, str(e)


def _persist_benchmarks(cfg: dict, benchmarks: list[dict],
                        seen_at: str) -> tuple[int, str]:
    """把 LLM 精选结果标记为 is_benchmark=1（不重复增加 capture_count）。"""
    try:
        db_path = database.resolve_db_path(cfg)
        database.init_db(db_path)
        count = database.upsert_benchmarks(
            benchmarks, db_path=db_path, seen_at=seen_at, bump_capture=False)
        return count, ""
    except Exception as e:
        return 0, str(e)


def _build_identity_index(apps: list[dict]) -> tuple[dict, dict]:
    """建立 apple_id 主键索引与规范化名称回退索引。"""
    by_id: dict[str, dict] = {}
    by_name: dict[str, dict] = {}
    for a in apps:
        aid = str(a.get("apple_id", "") or "").strip()
        if aid:
            by_id[aid] = a
        name = str(a.get("name", "") or "").strip().lower()
        if name and name not in by_name:
            by_name[name] = a
    return by_id, by_name


def _resolve_identity(pick: dict, by_id: dict, by_name: dict) -> tuple[dict | None, str]:
    """解析 LLM 选择对应的原始应用，返回（origin, 匹配方式）。"""
    aid = str(pick.get("apple_id", "") or "").strip()
    if aid and aid in by_id:
        return by_id[aid], "apple_id"
    name = str(pick.get("name", "") or "").strip().lower()
    if name and name in by_name:
        return by_name[name], "name_fallback"
    return None, "unmatched"


def _build_benchmark(pick: dict, origin: dict, match: str) -> dict:
    """合并 LLM 选择与原始榜单事实，产出标杆记录。"""
    apple_id = str(pick.get("apple_id") or origin.get("apple_id", "") or "").strip()
    return {
        "name": pick.get("name", "") or origin.get("name", ""),
        "artist": pick.get("artist", "") or origin.get("artist", ""),
        "apple_id": apple_id,
        "benchmark_key": f"apple:{apple_id}" if apple_id else
                         f"name:{str(pick.get('name', '')).strip().lower()}",
        "url": origin.get("url", ""),
        "rank": origin.get("rank"),
        "source_charts": origin.get("source", ""),  # 该标杆在哪些榜单出现
        "chart_hits": origin.get("source_charts") or [],
        "genre": origin.get("genre", ""),
        "description": origin.get("description", ""),
        "avg_rating": origin.get("avg_rating"),
        "rating_count": origin.get("rating_count"),
        "price": origin.get("price"),
        "release_date": origin.get("release_date", ""),
        "jtbd": pick.get("jtbd", ""),
        "jtbd_confidence": "name_only",  # RSS 字段无法证明功能细节，保守标注
        "search_keywords": pick.get("search_keywords", []),
        "pick_reason": pick.get("pick_reason", ""),
        "dev_complexity": pick.get("dev_complexity", ""),
        "monetization_hint": pick.get("monetization_hint", ""),
        "identity_match": match,
    }


def _now() -> str:
    """当前时间（UTC+8），所有数据快照统一用这个时间戳字段。"""
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")