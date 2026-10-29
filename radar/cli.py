# -*- coding: utf-8 -*-
"""CLI 入口: python -m radar <capture|probe|filter|database|gui>

流水线只有两步半：
- capture：拉 Apple 榜单 → LLM 精选 → 写 benchmarks.json + 本地数据库
- probe：检索鸿蒙供给 → 抓差评 → 提炼痛点 → 写 probe.json + 本地数据库
- filter：按需把数据库全量事实交给 LLM，按用户提示词挑机会（不再固定打分）

M4 版本化与熔断：
- 每次运行开启 `data/runs/<batch_id>/` 批次，产物与 LLM traces 归档其中；
- 单步重跑自动继承最近成功批次的上游产物；
- 任一阶段产出为空 / LLM 全失败时抛 PipelineAbortError，非零退出，
  且绝不覆写 data/ 顶层的有效历史产物。
"""
import argparse
import subprocess
import sys

from radar import database, store
from radar.config import load_config
from radar.pipeline import (
    EXIT_ABORT,
    EXIT_OK,
    EXIT_RETRYABLE,
    PipelineAbortError,
    RetryableAbortError,
)

# 命令 → (阶段名, 中文标签, 下一步提示)
COMMANDS = {
    "capture": ("capture", "榜单捕获",
                "下一步: python -m radar probe（检索鸿蒙同类供给与差评）"),
    "probe": ("probe", "供给探测",
              "下一步: python -m radar filter（按需 AI 筛选）"),
    "filter": ("filter", "AI 筛选", "结果: data/filter_results.json"),
}


def _build_parser():
    parser = argparse.ArgumentParser(
        prog="radar", description="纯血鸿蒙机会雷达——双端供给落差数据积累与按需筛选"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_stage(name, help_text):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--batch-id", default=None,
                       help="复用指定批次目录（全量扫描时各阶段共享同一批次）")
        p.add_argument("--base-run-id", default=None,
                       help="指定继承上游产物的来源批次；默认取最近成功批次")
        p.add_argument("--emit-events", action="store_true",
                       help="向 stdout 输出结构化遥测事件行（供 WebUI 实时消费）")
        return p

    add_stage("capture", "Apple 标杆捕获（拉榜单 → LLM 精选 → JTBD）")
    probe_parser = add_stage("probe", "鸿蒙供给探测（检索 → 语义过滤 → 差评痛点）")
    probe_parser.add_argument("--only-stale", action="store_true",
                              help="只重探数据库中超过刷新阈值的应用")
    probe_parser.add_argument("--apple-ids", default="",
                              help="只重探指定应用（逗号分隔的 Apple ID，供 WebUI 刷新选中）")
    add_stage("filter", "AI 事后筛选（数据库全量事实 → 候选机会）")

    db = sub.add_parser("database", help="本地增量数据库管理")
    db_sub = db.add_subparsers(dest="db_command", required=True)
    db_sub.add_parser("init", help="初始化数据库（创建表与索引）")
    db_sub.add_parser("stats", help="显示统计概览")
    db_sub.add_parser("refresh", help="列出待刷新的应用（超过阈值未 probe）")
    db_sub.add_parser("export", help="导出应用清单为 CSV")
    db_sub.add_parser("import", help="从现有 benchmarks/probe JSON 回填数据库")

    clean = db_sub.add_parser("clean", help="清理长期未出现的应用")
    clean.add_argument("--days", type=int, default=90,
                       help="删除超过 N 天未出现的应用（默认 90）")

    sub.add_parser("gui", help="启动 Streamlit 交互控制台")
    return parser


def _run_stage(stage: str, label: str, cfg: dict, batch_id: str | None,
               base_run_id: str | None, emit_events: bool = False,
               apple_ids: list[str] | None = None) -> int:
    """在版本化批次中执行一个阶段，返回产出数量。"""
    from radar import prompts
    batch = store.start_run(
        stage, batch_id=batch_id, base_run_id=base_run_id,
        cfg=cfg, prompts_snapshot=prompts.snapshot(), echo=emit_events,
    )
    print(f"[批次] {batch.batch_id}  ({label})")
    if batch.meta.get("inherited"):
        print(f"[继承] 来自 {batch.meta.get('base_run_id')}: "
              f"{', '.join(batch.meta['inherited'])}")

    try:
        if stage == "capture":
            from radar.pipeline import capture
            n = capture.run(cfg)
        elif stage == "probe":
            from radar.pipeline import probe
            n = probe.run(cfg, apple_ids=apple_ids)
        else:
            from radar.pipeline import filter
            n = filter.run(cfg)
    except RetryableAbortError as e:
        batch.finish("interrupted", summary="可重试中断", error=str(e))
        print(f"\n[中断] {e}", file=sys.stderr)
        return -EXIT_RETRYABLE
    except PipelineAbortError as e:
        batch.finish("failed", summary="熔断中止", error=str(e))
        print(f"\n[熔断] {e}", file=sys.stderr)
        return -EXIT_ABORT
    except Exception as e:
        batch.finish("failed", summary="执行异常", error=f"{type(e).__name__}: {e}")
        print(f"\n[失败] {type(e).__name__}: {e}", file=sys.stderr)
        return -EXIT_ABORT

    summary = f"{label}完成: {n}"
    batch.finish("success", summary=summary)
    print(f"[批次] {batch.batch_id} 已归档（data/runs/{batch.batch_id}/）")
    return n


def _database_command(args, cfg: dict) -> int:
    db_path = database.resolve_db_path(cfg)
    cmd = args.db_command
    if cmd == "init":
        path = database.init_db(db_path)
        print(f"数据库已初始化: {path}")
        return EXIT_OK
    if cmd == "stats":
        database.init_db(db_path)
        stats = database.get_stats(db_path)
        print(f"数据库: {db_path}")
        print(f"  应用总数     : {stats['total']}")
        print(f"  最近 7 天新增: {stats['new_7']}")
        print(f"  最近 30 天新增: {stats['new_30']}")
        print(f"  已 probe     : {stats['probed']}（其中空白 {stats['blank']}）")
        print(f"  待刷新       : {stats['stale']}（阈值 {stats['refresh_days']} 天）")
        print(f"  有高频/中频痛点: {stats['pain_apps']}")
        print(f"  竞品快照     : {stats['competitor_rows']} 条")
        print(f"  痛点记录     : {stats['review_rows']} 条")
        return EXIT_OK
    if cmd == "refresh":
        database.init_db(db_path)
        days = int((cfg.get("database") or {}).get("refresh_threshold_days", 30))
        stale = database.stale_apps(days, db_path=db_path)
        if not stale:
            print(f"没有超过 {days} 天未 probe 的应用。")
            return EXIT_OK
        print(f"待刷新 {len(stale)} 个应用（超过 {days} 天未 probe）：")
        for app in stale[:50]:
            print(f"  - {app['name']} ({app['apple_id']})  "
                  f"最近出现 {app['last_seen'][:10]}")
        print("\n在 WebUI 点「刷新待更新」或运行: python -m radar probe --only-stale")
        return EXIT_OK
    if cmd == "export":
        database.init_db(db_path)
        path = database.export_to_csv({}, db_path=db_path)
        print(f"已导出 CSV: {path}")
        return EXIT_OK
    if cmd == "import":
        path = database.init_db(db_path)
        result = database.import_from_artifacts(store.load_benchmarks(),
                                                store.load_probe(), db_path=db_path)
        print(f"已回填数据库 {path}: 应用 {result['apps']} 条，探测状态 {result['probed']} 条")
        return EXIT_OK
    if cmd == "clean":
        database.init_db(db_path)
        removed = database.clean_old_apps(args.days, db_path=db_path)
        print(f"已清理 {removed} 个超过 {args.days} 天未出现的应用")
        return EXIT_OK
    return EXIT_ABORT


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "gui":
        print("启动 Streamlit GUI...")
        subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py"])
        return EXIT_OK

    cfg = load_config()

    if args.command == "database":
        return _database_command(args, cfg)

    stage, label, next_hint = COMMANDS[args.command]
    apple_ids = None
    if stage == "probe" and getattr(args, "apple_ids", ""):
        apple_ids = [a.strip() for a in str(args.apple_ids).split(",") if a.strip()]
        print(f"[刷新] 仅重探指定 {len(apple_ids)} 个应用")
    elif stage == "probe" and getattr(args, "only_stale", False):
        db_path = database.resolve_db_path(cfg)
        database.init_db(db_path)
        days = int((cfg.get("database") or {}).get("refresh_threshold_days", 30))
        apple_ids = [a["apple_id"] for a in database.stale_apps(days, db_path=db_path)]
        if not apple_ids:
            print(f"没有超过 {days} 天未 probe 的应用，无需刷新。")
            return EXIT_OK
        print(f"[刷新] 仅重探 {len(apple_ids)} 个待更新应用")

    n = _run_stage(stage, label, cfg, args.batch_id, args.base_run_id,
                   emit_events=args.emit_events, apple_ids=apple_ids)
    if n < 0:
        # 负数编码失败性质：-1 致命熔断，-2 可重试中断 → 转成正退出码
        return abs(n)

    print(f"\n=== {stage} 完成: {n} 条 ===\n{next_hint}")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
