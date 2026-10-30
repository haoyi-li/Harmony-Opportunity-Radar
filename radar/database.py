# -*- coding: utf-8 -*-
"""SQLite 增量数据库：把每次 capture / probe 的事实累积为可查询的数据资产。

设计原则（见计划 1793290050268）:
- 三张表：apps（应用基础信息）、competitors（竞品历史快照）、reviews（差评痛点）。
- 只增不减：同一 apple_id 重复 capture 只更新 last_seen / capture_count，
  competitors 每轮 probe 追加新快照（probed_at 区分轮次），保留历史趋势。
- 写入失败绝不影响 JSON 产物生成：所有写操作由调用方 try/except 包裹，
  本模块自身也不在写路径上抛致命错误以外的异常（结构错误照常抛出以便测试）。
- 并发安全：启用 WAL（多读一写）+ busy_timeout，短连接、用完即关。
"""
import csv
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from radar.config import DATA_DIR, PROJECT_ROOT

DEFAULT_DB_PATH = DATA_DIR / "apps.db"
DEFAULT_DECISIONS_DB_PATH = DATA_DIR / "decisions.db"

# 高频/中频/低频 → 展示排序权重
FREQ_ORDER = {"高频": 0, "中频": 1, "低频": 2}

SCHEMA = """
CREATE TABLE IF NOT EXISTS apps (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  apple_id TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  artist TEXT,
  jtbd TEXT,
  pick_reason TEXT,
  search_keywords TEXT,
  url TEXT,
  source_charts TEXT,
  first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  capture_count INTEGER DEFAULT 1,
  last_probe_at TEXT,
  needs_refresh INTEGER DEFAULT 0,
  is_blank INTEGER DEFAULT 0,
  probe_status TEXT,
  probe_evidence_grade TEXT,
  competitor_count INTEGER DEFAULT 0,
  is_favorite INTEGER DEFAULT 0,
  -- 目录层详情（iTunes lookup 补齐，不参与 LLM 精选）
  genre TEXT,
  description TEXT,
  avg_rating REAL,
  rating_count INTEGER,
  price REAL,
  release_date TEXT,
  -- 榜单事实与增量快照
  best_rank INTEGER,
  chart_hits TEXT,
  is_benchmark INTEGER DEFAULT 0,
  metrics_version INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_apps_apple_id ON apps(apple_id);
CREATE INDEX IF NOT EXISTS idx_apps_last_seen ON apps(last_seen);
CREATE INDEX IF NOT EXISTS idx_apps_needs_refresh ON apps(needs_refresh);

CREATE TABLE IF NOT EXISTS app_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  apple_id TEXT NOT NULL,
  captured_at TEXT NOT NULL,
  avg_rating REAL,
  rating_count INTEGER,
  best_rank INTEGER,
  chart_hits TEXT,
  metrics_version INTEGER,
  FOREIGN KEY (apple_id) REFERENCES apps(apple_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_snapshot_apple_id
  ON app_snapshots(apple_id, captured_at DESC);

CREATE TABLE IF NOT EXISTS competitors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  apple_id TEXT NOT NULL,
  package TEXT NOT NULL,
  name TEXT,
  category TEXT,
  intro TEXT,
  downloads TEXT,
  rating REAL,
  version TEXT,
  update_time TEXT,
  review_evidence TEXT,
  bad_review_count INTEGER DEFAULT 0,
  probed_at TEXT NOT NULL,
  FOREIGN KEY (apple_id) REFERENCES apps(apple_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_comp_apple_id ON competitors(apple_id);
CREATE INDEX IF NOT EXISTS idx_comp_probed_at ON competitors(probed_at);

CREATE TABLE IF NOT EXISTS reviews (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  competitor_id INTEGER NOT NULL,
  issue TEXT NOT NULL,
  frequency TEXT,
  quote TEXT,
  collected_at TEXT NOT NULL,
  FOREIGN KEY (competitor_id) REFERENCES competitors(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_rev_competitor ON reviews(competitor_id);

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
"""

# 产物库：筛选裁决、批次记录与知识模式。与 apps.db 单向依赖：
# filter 读 apps.db → 写 decisions.db；apps.db 永远不引用 decisions.db。
DECISIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS app_verdicts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  apple_id TEXT NOT NULL,
  verdict TEXT NOT NULL,
  score REAL,
  reason TEXT,
  attack_vector TEXT,
  native_features TEXT,
  competitors_summary TEXT,
  basis TEXT NOT NULL,
  app_metrics_version INTEGER,
  probe_snapshot_at TEXT,
  competitor_count_snapshot INTEGER,
  criteria_hash TEXT,
  model TEXT NOT NULL,
  batch_id TEXT,
  filtered_at TEXT NOT NULL,
  expires_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_verdict_apple_id
  ON app_verdicts(apple_id, filtered_at DESC);
CREATE INDEX IF NOT EXISTS idx_verdict_expires ON app_verdicts(expires_at);

CREATE TABLE IF NOT EXISTS filter_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  model TEXT NOT NULL,
  criteria_hash TEXT,
  input_count INTEGER,
  accept_count INTEGER,
  reject_count INTEGER,
  cached_count INTEGER,
  started_at TEXT NOT NULL,
  finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_filter_runs_batch ON filter_runs(batch_id);

CREATE TABLE IF NOT EXISTS knowledge_patterns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  genre TEXT NOT NULL,
  pattern_type TEXT NOT NULL,
  pattern_summary TEXT NOT NULL,
  evidence_count INTEGER DEFAULT 1,
  extracted_at TEXT NOT NULL,
  last_updated TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pattern_genre
  ON knowledge_patterns(genre, pattern_type);
"""


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def resolve_db_path(cfg: dict | None = None) -> Path:
    """解析配置中的数据库路径（相对项目根目录）。"""
    raw = ""
    if isinstance(cfg, dict):
        raw = str((cfg.get("database") or {}).get("path", "") or "").strip()
    if not raw:
        return DEFAULT_DB_PATH
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def resolve_decisions_db_path(cfg: dict | None = None) -> Path:
    """解析产物库路径（decisions.db），默认 data/decisions.db。"""
    raw = ""
    if isinstance(cfg, dict):
        raw = str((cfg.get("database") or {}).get("decisions_path", "") or "").strip()
    if not raw:
        return DEFAULT_DECISIONS_DB_PATH
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def connect(db_path=None) -> sqlite3.Connection:
    """打开数据库连接（WAL + busy_timeout），调用方负责 close。"""
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
    except sqlite3.Error:
        pass
    return conn


def init_db(db_path=None) -> Path:
    """初始化数据库：创建表与索引，并对既有库做增量迁移（幂等）。"""
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
    migrate_schema(path)
    return path


# 既有数据库需要补齐的目录层 / 快照字段。SQLite 只支持 ADD COLUMN。
_MIGRATION_COLUMNS = {
    "genre": "TEXT",
    "description": "TEXT",
    "avg_rating": "REAL",
    "rating_count": "INTEGER",
    "price": "REAL",
    "release_date": "TEXT",
    "best_rank": "INTEGER",
    "chart_hits": "TEXT",
    "is_benchmark": "INTEGER DEFAULT 0",
    "metrics_version": "INTEGER DEFAULT 1",
}


def migrate_schema(db_path=None) -> list[str]:
    """增量 Schema 迁移：只添加列/表，不删除、不重命名，保留现有数据。

    返回实际新增的列名列表（首次迁移便于在日志中展示）。
    """
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    conn = connect(path)
    added: list[str] = []
    try:
        existing_cols = {row[1] for row in
                         conn.execute("PRAGMA table_info(apps)").fetchall()}
        for col, typ in _MIGRATION_COLUMNS.items():
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE apps ADD COLUMN {col} {typ}")
                added.append(col)

        conn.executescript("""
        CREATE TABLE IF NOT EXISTS app_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          apple_id TEXT NOT NULL,
          captured_at TEXT NOT NULL,
          avg_rating REAL,
          rating_count INTEGER,
          best_rank INTEGER,
          chart_hits TEXT,
          metrics_version INTEGER,
          FOREIGN KEY (apple_id) REFERENCES apps(apple_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_snapshot_apple_id
          ON app_snapshots(apple_id, captured_at DESC);
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_apps_benchmark "
                     "ON apps(is_benchmark)")

        # 回填：老库中没有 is_benchmark/metrics_version 的应用。
        conn.execute(
            "UPDATE apps SET metrics_version=1 "
            "WHERE metrics_version IS NULL OR metrics_version < 1")
        conn.execute(
            "UPDATE apps SET is_benchmark=1 "
            "WHERE (is_benchmark IS NULL OR is_benchmark=0) "
            "AND jtbd IS NOT NULL AND TRIM(jtbd) <> ''")

        # 回填 chart_hits：历史库只有逗号分隔的 source_charts 字符串。
        rows = conn.execute(
            "SELECT apple_id, source_charts FROM apps "
            "WHERE (chart_hits IS NULL OR chart_hits='') "
            "AND source_charts IS NOT NULL AND TRIM(source_charts) <> ''"
        ).fetchall()
        for row in rows:
            hits = _chart_list(row["source_charts"])
            if hits:
                conn.execute(
                    "UPDATE apps SET chart_hits=? WHERE apple_id=?",
                    (json.dumps(hits, ensure_ascii=False), row["apple_id"]))
        conn.commit()
    except sqlite3.Error:
        conn.rollback()
        raise
    finally:
        conn.close()
    if added:
        print("  [migrate] 已为 apps 表补充字段: " + ", ".join(added))
    return added


def init_decisions_db(db_path=None) -> Path:
    """初始化产物库（decisions.db）：裁决、批次与知识模式表（幂等）。"""
    path = Path(db_path) if db_path else DEFAULT_DECISIONS_DB_PATH
    conn = connect(path)
    try:
        conn.executescript(DECISIONS_SCHEMA)
        conn.commit()
    finally:
        conn.close()
    return path


# ============================================================
# 写入：capture
# ============================================================
def _chart_list(value) -> list[str]:
    """把 source_charts / chart_hits 统一规范为去重后的榜单列表。"""
    if isinstance(value, (list, tuple, set)):
        items = list(value)
    elif isinstance(value, str):
        items = value.replace("，", ",").split(",")
    else:
        return []
    result: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def _stored_chart_list(value) -> list[str]:
    """解析数据库里以 JSON 文本存储的 chart_hits（兼容旧的逗号字符串）。"""
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                return _chart_list(json.loads(text))
            except (TypeError, ValueError):
                pass
    return _chart_list(value)


def _to_int(val) -> int | None:
    if val is None or isinstance(val, bool):
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _metrics_changed(old: sqlite3.Row, app: dict, best_rank: int | None,
                     chart_hits: list[str]) -> bool:
    """判断是否需要追加指标快照。

    - avg_rating / best_rank / 榜单覆盖变化：任意变化即记录；
    - rating_count：只有变化超过 1%（且至少 20 条）才记录，避免每天为
      几十条新增评论写一行快照，把 800 个应用的表撑爆。
    """
    new_rating = _to_float(app.get("avg_rating"))
    old_rating = _to_float(old["avg_rating"])
    if new_rating is not None and new_rating != old_rating:
        return True

    new_count = _to_int(app.get("rating_count"))
    old_count = _to_int(old["rating_count"])
    if new_count is not None:
        if old_count is None:
            return True
        if new_count != old_count and abs(new_count - old_count) >= max(
                20, int(old_count * 0.01)):
            return True

    if best_rank is not None and best_rank != _to_int(old["best_rank"]):
        return True

    old_hits = set(_stored_chart_list(old["chart_hits"]))
    if set(chart_hits) - old_hits:
        return True
    return False


def upsert_app(benchmark: dict, db_path=None, seen_at: str | None = None,
               *, _conn: sqlite3.Connection | None = None,
               _commit: bool = True, bump_capture: bool = True,
               is_benchmark: bool | None = None) -> str | None:
    """插入或更新 apps 表，返回 apple_id（无 apple_id 时返回 None，不入库）。

    - 目录层应用（is_benchmark=0）与 LLM 精选标杆（is_benchmark=1）共用本函数；
    - 已存在的应用只刷新 last_seen / capture_count 与可能变动的文本字段，
      不重置 last_probe_at / is_blank / competitor_count 等 probe 事实；
    - rating / rating_count / best_rank / 榜单覆盖变化时追加 app_snapshots。
    """
    app = benchmark or {}
    apple_id = str(app.get("apple_id", "") or "").strip()
    name = str(app.get("name", "") or "").strip()
    if not apple_id or not name:
        return None
    now = seen_at or _now()
    keywords = app.get("search_keywords") or []
    if not isinstance(keywords, list):
        keywords = []
    owned_conn = _conn is None
    conn = _conn or connect(db_path)
    try:
        old = conn.execute("SELECT * FROM apps WHERE apple_id=?",
                           (apple_id,)).fetchone()
        raw_charts = app.get("source_charts")
        source_charts_text = (", ".join(_chart_list(raw_charts))
                              if isinstance(raw_charts, (list, tuple, set))
                              else str(raw_charts or ""))
        chart_hits = _chart_list(app.get("chart_hits") or raw_charts)
        if old is not None:
            chart_hits = _chart_list(_stored_chart_list(old["chart_hits"]) + chart_hits)
        new_rank = _to_int(app.get("rank"))
        old_rank = _to_int(old["best_rank"]) if old is not None else None
        ranked = [r for r in (old_rank, new_rank) if r is not None]
        best_rank = min(ranked) if ranked else None

        version = (_to_int(old["metrics_version"]) or 1) if old is not None else 1
        changed = old is None
        if old is not None and _metrics_changed(old, app, best_rank, chart_hits):
            changed = True
            version += 1

        flag = 1 if is_benchmark else (1 if app.get("is_benchmark") else 0)
        conn.execute(
            """
            INSERT INTO apps (
              apple_id, name, artist, jtbd, pick_reason, search_keywords,
              url, source_charts, genre, description, avg_rating, rating_count,
              price, release_date, best_rank, chart_hits, is_benchmark,
              metrics_version, first_seen, last_seen, capture_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, 1)
            ON CONFLICT(apple_id) DO UPDATE SET
              name=excluded.name,
              artist=COALESCE(NULLIF(excluded.artist, ''), apps.artist),
              jtbd=COALESCE(NULLIF(excluded.jtbd, ''), apps.jtbd),
              pick_reason=COALESCE(NULLIF(excluded.pick_reason, ''), apps.pick_reason),
              search_keywords=COALESCE(NULLIF(excluded.search_keywords, '[]'),
                                       apps.search_keywords),
              url=COALESCE(NULLIF(excluded.url, ''), apps.url),
              source_charts=COALESCE(NULLIF(excluded.source_charts, ''),
                                     apps.source_charts),
              genre=COALESCE(NULLIF(excluded.genre, ''), apps.genre),
              description=COALESCE(NULLIF(excluded.description, ''),
                                   apps.description),
              avg_rating=COALESCE(excluded.avg_rating, apps.avg_rating),
              rating_count=COALESCE(excluded.rating_count, apps.rating_count),
              price=COALESCE(excluded.price, apps.price),
              release_date=COALESCE(NULLIF(excluded.release_date, ''),
                                    apps.release_date),
              best_rank=COALESCE(excluded.best_rank, apps.best_rank),
              chart_hits=excluded.chart_hits,
              is_benchmark=MAX(apps.is_benchmark, excluded.is_benchmark),
              metrics_version=excluded.metrics_version,
              last_seen=excluded.last_seen,
              capture_count=apps.capture_count + ?
            """,
            (
                apple_id, name,
                app.get("artist", ""),
                app.get("jtbd", ""),
                app.get("pick_reason", ""),
                json.dumps(keywords, ensure_ascii=False),
                app.get("url", ""),
                source_charts_text,
                app.get("genre", ""),
                app.get("description", ""),
                _to_float(app.get("avg_rating")),
                _to_int(app.get("rating_count")),
                _to_float(app.get("price")),
                app.get("release_date", ""),
                best_rank,
                json.dumps(chart_hits, ensure_ascii=False),
                flag,
                version,
                now, now,
                1 if bump_capture else 0,
            ),
        )
        if changed:
            conn.execute(
                """
                INSERT INTO app_snapshots (
                  apple_id, captured_at, avg_rating, rating_count, best_rank,
                  chart_hits, metrics_version)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (apple_id, now,
                 _to_float(app.get("avg_rating")) if app.get("avg_rating") is not None
                 else (old["avg_rating"] if old is not None else None),
                 _to_int(app.get("rating_count")) if app.get("rating_count") is not None
                 else (old["rating_count"] if old is not None else None),
                 best_rank,
                 json.dumps(chart_hits, ensure_ascii=False),
                 version),
            )
        if owned_conn and _commit:
            conn.commit()
    finally:
        if owned_conn:
            conn.close()
    return apple_id


def upsert_catalog_apps(apps: list[dict], db_path=None,
                        seen_at: str | None = None) -> dict:
    """把一轮 RSS 全量目录写入 apps 表（is_benchmark 保持 0）。

    返回 {"apps": 入库数, "snapshots": 快照数}；单连接批量执行，避免
    800 个应用各开一次 SQLite 连接。
    """
    now = seen_at or _now()
    valid = [a for a in (apps or []) if str(a.get("apple_id", "")).strip()
             and str(a.get("name", "")).strip()]
    if not valid:
        return {"apps": 0, "snapshots": 0}
    conn = connect(db_path)
    try:
        before = _scalar(conn, "SELECT COUNT(*) FROM app_snapshots")
        count = 0
        for app in valid:
            if upsert_app(app, seen_at=now, _conn=conn, _commit=False):
                count += 1
        conn.commit()
        after = _scalar(conn, "SELECT COUNT(*) FROM app_snapshots")
        return {"apps": count, "snapshots": max(0, int(after) - int(before))}
    finally:
        conn.close()


def upsert_benchmarks(benchmarks: list[dict], db_path=None,
                      seen_at: str | None = None, bump_capture: bool = True) -> int:
    """批量写入 capture 精选结果并标记 is_benchmark=1，返回成功入库条数。"""
    now = seen_at or _now()
    if not (benchmarks or []):
        return 0
    conn = connect(db_path)
    try:
        count = 0
        for bm in benchmarks or []:
            if upsert_app(bm, seen_at=now, _conn=conn, _commit=False,
                          bump_capture=bump_capture, is_benchmark=True):
                count += 1
        conn.commit()
        return count
    finally:
        conn.close()


# ============================================================
# 写入：probe
# ============================================================
def add_competitors(apple_id: str, competitors: list[dict], probed_at: str,
                    db_path=None) -> list[int]:
    """追加一轮竞品快照及其痛点，返回新插入的 competitor id 列表。

    同一轮内相同 package 只保留第一条，避免重复计数。
    """
    apple_id = str(apple_id or "").strip()
    if not apple_id:
        return []
    conn = connect(db_path)
    inserted: list[int] = []
    try:
        seen_pkg: set[str] = set()
        for comp in competitors or []:
            pkg = str((comp or {}).get("package", "") or "").strip()
            if not pkg or pkg in seen_pkg:
                continue
            seen_pkg.add(pkg)
            rating = _to_float(comp.get("score"))
            cur = conn.execute(
                """
                INSERT INTO competitors (apple_id, package, name, category, intro,
                                         downloads, rating, version, update_time,
                                         review_evidence, bad_review_count, probed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    apple_id, pkg,
                    comp.get("name", ""),
                    comp.get("category", ""),
                    comp.get("intro", ""),
                    comp.get("downloads", ""),
                    rating,
                    comp.get("version", ""),
                    comp.get("update_time", ""),
                    comp.get("review_evidence", ""),
                    int(comp.get("bad_review_count", 0) or 0),
                    probed_at,
                ),
            )
            comp_id = int(cur.lastrowid or 0)
            inserted.append(comp_id)
            _insert_pains(conn, comp_id, comp.get("pain_points"), probed_at)
        conn.commit()
    finally:
        conn.close()
    return inserted


def _insert_pains(conn: sqlite3.Connection, competitor_id: int, pains, collected_at: str):
    if not isinstance(pains, list):
        return
    for pain in pains:
        if not isinstance(pain, dict):
            continue
        issue = str(pain.get("issue", "") or "").strip()
        if not issue:
            continue
        conn.execute(
            "INSERT INTO reviews (competitor_id, issue, frequency, quote, collected_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (competitor_id, issue, pain.get("frequency", ""),
             pain.get("quote", ""), collected_at),
        )


def update_probe_state(apple_id: str, probe_record: dict, probed_at: str,
                       db_path=None) -> bool:
    """回写 apps 表的 probe 事实（时间、状态、证据等级、竞品数、是否空白）。"""
    apple_id = str(apple_id or "").strip()
    if not apple_id:
        return False
    record = probe_record or {}
    conn = connect(db_path)
    try:
        cur = conn.execute(
            """
            UPDATE apps SET
              last_probe_at=?, is_blank=?, probe_status=?, probe_evidence_grade=?,
              competitor_count=?, needs_refresh=0
            WHERE apple_id=?
            """,
            (
                probed_at,
                1 if record.get("blank_signal") else 0,
                record.get("probe_status", ""),
                record.get("evidence_grade", ""),
                int(record.get("competitor_count", 0) or 0),
                apple_id,
            ),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def save_probe_record(probe_record: dict, probed_at: str, db_path=None) -> list[int]:
    """把一条 probe 记录整体落库：竞品+痛点 → 回写应用状态。"""
    apple_id = str((probe_record or {}).get("apple_id", "") or "").strip()
    if not apple_id:
        return []
    ids = add_competitors(apple_id, probe_record.get("competitors") or [],
                          probed_at, db_path=db_path)
    update_probe_state(apple_id, probe_record, probed_at, db_path=db_path)
    return ids


# ============================================================
# 刷新标记
# ============================================================
def mark_needs_refresh(days: int = 30, db_path=None) -> int:
    """标记「超过 N 天未 probe（或从未 probe）」的应用为待刷新，返回条数。"""
    cutoff = _days_ago(days)
    conn = connect(db_path)
    try:
        cur = conn.execute(
            """
            UPDATE apps SET needs_refresh=1
            WHERE last_probe_at IS NULL OR last_probe_at < ?
            """,
            (cutoff,),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def stale_apps(days: int = 30, db_path=None) -> list[dict]:
    """返回超过 N 天未 probe 的应用（用于「刷新全部待刷新」）。"""
    cutoff = _days_ago(days)
    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM apps WHERE last_probe_at IS NULL OR last_probe_at < ?"
            " ORDER BY last_seen DESC",
            (cutoff,),
        ).fetchall()
        return [_app_row(r) for r in rows]
    finally:
        conn.close()


def _days_ago(days: int) -> str:
    try:
        days = max(0, int(days))
    except (TypeError, ValueError):
        days = 30
    return (datetime.now(timezone.utc).astimezone()
            - timedelta(days=days)).isoformat(timespec="seconds")


# ============================================================
# 查询
# ============================================================
def _app_row(row: sqlite3.Row) -> dict:
    data = dict(row)
    try:
        data["search_keywords"] = json.loads(data.get("search_keywords") or "[]")
    except (ValueError, TypeError):
        data["search_keywords"] = []
    try:
        data["chart_hits"] = _stored_chart_list(data.get("chart_hits"))
        if not data["chart_hits"]:
            data["chart_hits"] = _chart_list(data.get("source_charts"))
    except (ValueError, TypeError):
        data["chart_hits"] = _chart_list(data.get("source_charts"))
    data["is_blank"] = bool(data.get("is_blank"))
    data["needs_refresh"] = bool(data.get("needs_refresh"))
    data["is_favorite"] = bool(data.get("is_favorite"))
    data["is_benchmark"] = bool(data.get("is_benchmark"))
    data["metrics_version"] = int(data.get("metrics_version") or 1)
    return data


def get_apps(filters: dict | None = None, db_path=None) -> list[dict]:
    """按条件查询应用列表（附带竞品数/平均评分/高频痛点数派生列）。

    filters 支持的键（全部可选）:
      days           仅看最近 N 天出现
      since / until  ISO 时间范围（覆盖 days）
      competitor     "0" | "1-2" | "3+" 竞品数量区间
      rating         "none" | "low" | "mid" | "high" 竞品平均评分区间
      pain           "none" | "high" | "mid" 差评痛点强度
      refresh        "all" | "stale" 刷新状态
      favorite       True 仅收藏
      query          应用名/JTBD 模糊搜索
      limit          返回条数上限（默认 500）
    """
    filters = filters or {}
    where: list[str] = []
    params: list = []

    days = filters.get("days")
    if days not in (None, "", "all"):
        where.append("a.last_seen >= ?")
        params.append(_days_ago(int(days)))
    if filters.get("since"):
        where.append("a.last_seen >= ?")
        params.append(str(filters["since"]))
    if filters.get("until"):
        where.append("a.last_seen <= ?")
        params.append(str(filters["until"]))

    comp = filters.get("competitor")
    if comp == "0":
        where.append("a.competitor_count = 0")
    elif comp == "1-2":
        where.append("a.competitor_count BETWEEN 1 AND 2")
    elif comp == "3+":
        where.append("a.competitor_count >= 3")

    if filters.get("refresh") == "stale":
        # 以时间为准而非缓存标记：标记只在 capture 时刷新，界面需要实时准确
        where.append("(a.last_probe_at IS NULL OR a.last_probe_at < ?)")
        params.append(_days_ago(_refresh_days(db_path)))
    if filters.get("favorite"):
        where.append("a.is_favorite = 1")
    if filters.get("query"):
        where.append("(a.name LIKE ? OR a.jtbd LIKE ? OR a.artist LIKE ?)")
        like = f"%{filters['query']}%"
        params.extend([like, like, like])

    # 派生列只统计「最近一轮 probe」的事实：历史轮次留在详情页做趋势，
    # 不参与当前状态判断（否则下架竞品会让竞品数虚高、评分被旧值拖偏）。
    sql = """
    SELECT a.*,
      (SELECT COUNT(DISTINCT c.package) FROM competitors c
        WHERE c.apple_id=a.apple_id AND c.probed_at=a.last_probe_at) AS comp_total,
      (SELECT AVG(c.rating) FROM competitors c
        WHERE c.apple_id=a.apple_id AND c.probed_at=a.last_probe_at
          AND c.rating IS NOT NULL) AS comp_avg_rating,
      (SELECT COUNT(*) FROM reviews r JOIN competitors c ON r.competitor_id=c.id
        WHERE c.apple_id=a.apple_id AND c.probed_at=a.last_probe_at
          AND r.frequency='高频') AS high_pain_count,
      (SELECT COUNT(*) FROM reviews r JOIN competitors c ON r.competitor_id=c.id
        WHERE c.apple_id=a.apple_id AND c.probed_at=a.last_probe_at
          AND r.frequency='中频') AS mid_pain_count,
      (SELECT COUNT(*) FROM reviews r JOIN competitors c ON r.competitor_id=c.id
        WHERE c.apple_id=a.apple_id AND c.probed_at=a.last_probe_at) AS pain_count
    FROM apps a
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY a.last_seen DESC"

    limit = filters.get("limit")
    conn = connect(db_path)
    try:
        rows = conn.execute(sql, params).fetchall()
        apps = [_app_row(r) for r in rows]
    finally:
        conn.close()

    apps = [_apply_derived_filters(a, filters) for a in apps]
    apps = [a for a in apps if a is not None]
    if isinstance(limit, int) and limit > 0:
        apps = apps[:limit]
    return apps


def _apply_derived_filters(app: dict, filters: dict) -> dict | None:
    rating = filters.get("rating")
    avg = app.get("comp_avg_rating")
    if rating == "none":
        if app.get("comp_total"):
            return None
    elif rating == "low":
        if avg is None or not (avg < 3.5):
            return None
    elif rating == "mid":
        if avg is None or not (3.5 <= avg < 4.0):
            return None
    elif rating == "high":
        if avg is None or not (avg >= 4.0):
            return None

    pain = filters.get("pain")
    if pain == "none":
        if app.get("pain_count"):
            return None
    elif pain == "high":
        if not app.get("high_pain_count"):
            return None
    elif pain == "mid":
        if not (app.get("mid_pain_count") or app.get("high_pain_count")):
            return None
    return app


def get_app_detail(apple_id: str, db_path=None) -> dict | None:
    """单个应用详情：基础信息 + 每轮 probe 的竞品快照（含痛点），按时间倒序。"""
    apple_id = str(apple_id or "").strip()
    if not apple_id:
        return None
    conn = connect(db_path)
    try:
        row = conn.execute("SELECT * FROM apps WHERE apple_id=?", (apple_id,)).fetchone()
        if row is None:
            return None
        app = _app_row(row)
        comp_rows = conn.execute(
            "SELECT * FROM competitors WHERE apple_id=? ORDER BY probed_at DESC, id DESC",
            (apple_id,),
        ).fetchall()
        rev_rows = conn.execute(
            """
            SELECT r.*, c.package AS package FROM reviews r
            JOIN competitors c ON r.competitor_id=c.id
            WHERE c.apple_id=?
            ORDER BY r.collected_at DESC, r.id DESC
            """,
            (apple_id,),
        ).fetchall()
    finally:
        conn.close()

    reviews_by_comp: dict[int, list[dict]] = {}
    for r in rev_rows:
        reviews_by_comp.setdefault(int(r["competitor_id"]), []).append(dict(r))

    competitors = []
    for c in comp_rows:
        item = dict(c)
        pains = reviews_by_comp.get(int(c["id"]), [])
        item["pain_points"] = sorted(
            pains, key=lambda p: FREQ_ORDER.get(str(p.get("frequency") or ""), 3))
        competitors.append(item)

    rounds = _group_rounds(competitors)
    app["competitors"] = competitors
    app["rounds"] = rounds
    return app


def _group_rounds(competitors: list[dict]) -> list[dict]:
    """把竞品快照按 probed_at 聚合成 probe 历史时间线。"""
    rounds: dict[str, dict] = {}
    for c in competitors:
        key = c.get("probed_at", "")
        bucket = rounds.setdefault(key, {
            "probed_at": key,
            "competitors": [],
            "avg_rating": None,
            "high_pain_count": 0,
        })
        bucket["competitors"].append({
            "name": c.get("name", ""),
            "package": c.get("package", ""),
            "rating": c.get("rating"),
            "downloads": c.get("downloads", ""),
            "bad_review_count": c.get("bad_review_count", 0),
        })
        bucket["high_pain_count"] += sum(
            1 for p in c.get("pain_points", []) if p.get("frequency") == "高频")

    result = []
    for key in sorted(rounds, reverse=True):
        bucket = rounds[key]
        ratings = [c["rating"] for c in bucket["competitors"]
                   if isinstance(c["rating"], (int, float))]
        bucket["avg_rating"] = round(sum(ratings) / len(ratings), 2) if ratings else None
        bucket["competitor_count"] = len({c["package"] for c in bucket["competitors"]})
        result.append(bucket)
    return result


def get_stats(db_path=None) -> dict:
    """概览统计：总量、7/30 天新增、待刷新、空白、有竞品、痛点覆盖。"""
    conn = connect(db_path)
    try:
        total = _scalar(conn, "SELECT COUNT(*) FROM apps")
        new_7 = _scalar(conn, "SELECT COUNT(*) FROM apps WHERE first_seen >= ?",
                        (_days_ago(7),))
        new_30 = _scalar(conn, "SELECT COUNT(*) FROM apps WHERE first_seen >= ?",
                         (_days_ago(30),))
        stale = _scalar(
            conn,
            "SELECT COUNT(*) FROM apps WHERE last_probe_at IS NULL OR last_probe_at < ?",
            (_days_ago(_refresh_days(db_path)),))
        blank = _scalar(conn, "SELECT COUNT(*) FROM apps WHERE is_blank=1")
        with_comp = _scalar(conn, "SELECT COUNT(*) FROM apps WHERE competitor_count > 0")
        probed = _scalar(conn, "SELECT COUNT(*) FROM apps WHERE last_probe_at IS NOT NULL")
        pain_apps = _scalar(conn, """
            SELECT COUNT(DISTINCT c.apple_id) FROM reviews r
            JOIN competitors c ON r.competitor_id=c.id
            WHERE r.frequency IN ('高频','中频')""")
        favorites = _scalar(conn, "SELECT COUNT(*) FROM apps WHERE is_favorite=1")
        benchmarks = _scalar(conn, "SELECT COUNT(*) FROM apps WHERE is_benchmark=1")
        snapshots = _scalar(conn, "SELECT COUNT(*) FROM app_snapshots")
        competitors = _scalar(conn, "SELECT COUNT(*) FROM competitors")
        reviews = _scalar(conn, "SELECT COUNT(*) FROM reviews")
        last_capture = _scalar(conn, "SELECT MAX(last_seen) FROM apps") or ""
        last_probe = _scalar(conn, "SELECT MAX(probed_at) FROM competitors") or ""
    finally:
        conn.close()
    return {
        "total": total,
        "new_7": new_7,
        "new_30": new_30,
        "stale": stale,
        "blank": blank,
        "with_competitors": with_comp,
        "probed": probed,
        "unprobed": int(total) - int(probed),
        "pain_apps": pain_apps,
        "favorites": favorites,
        "benchmarks": benchmarks,
        "catalog": int(total) - int(benchmarks),
        "snapshots": snapshots,
        "competitor_rows": competitors,
        "review_rows": reviews,
        "last_capture": last_capture,
        "last_probe": last_probe,
        "refresh_days": _refresh_days(db_path),
    }


def _refresh_days(db_path=None) -> int:
    try:
        from radar.config import load_config
        return int((load_config().get("database") or {}).get("refresh_threshold_days", 30))
    except Exception:
        return 30


def _scalar(conn: sqlite3.Connection, sql: str, params=()) -> int | str:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        return 0
    value = row[0]
    return value if value is not None else 0


def toggle_favorite(apple_id: str, db_path=None) -> bool:
    """切换收藏标记，返回切换后的状态。"""
    apple_id = str(apple_id or "").strip()
    if not apple_id:
        return False
    conn = connect(db_path)
    try:
        row = conn.execute("SELECT is_favorite FROM apps WHERE apple_id=?",
                           (apple_id,)).fetchone()
        if row is None:
            return False
        new = 0 if row[0] else 1
        conn.execute("UPDATE apps SET is_favorite=? WHERE apple_id=?", (new, apple_id))
        conn.commit()
        return bool(new)
    finally:
        conn.close()


def _attach_latest_competitors(conn: sqlite3.Connection, apps: list[dict],
                               per_app_limit: int = 3,
                               pains_per_comp: int = 3) -> list[dict]:
    """为应用列表附上「最近一轮 probe」的竞品与痛点摘要（就地修改并返回）。"""
    if not apps:
        return apps
    ids = [a["apple_id"] for a in apps]
    placeholders = ",".join("?" * len(ids))
    comp_rows = conn.execute(
        f"""
        SELECT c.* FROM competitors c JOIN apps a ON a.apple_id=c.apple_id
        WHERE a.apple_id IN ({placeholders}) AND c.probed_at=a.last_probe_at
        ORDER BY c.rating DESC, c.id DESC
        """,
        ids,
    ).fetchall()
    rev_rows = conn.execute(
        f"""
        SELECT r.*, c.apple_id AS apple_id FROM reviews r
        JOIN competitors c ON r.competitor_id=c.id
        WHERE c.apple_id IN ({placeholders}) AND c.probed_at=(SELECT last_probe_at
          FROM apps WHERE apple_id=c.apple_id)
        """,
        ids,
    ).fetchall()

    pains_by_comp: dict[int, list[dict]] = {}
    for r in rev_rows:
        pains_by_comp.setdefault(int(r["competitor_id"]), []).append(dict(r))

    comps_by_app: dict[str, list[dict]] = {}
    for c in comp_rows:
        bucket = comps_by_app.setdefault(c["apple_id"], [])
        if len(bucket) >= per_app_limit:
            continue
        pains = pains_by_comp.get(int(c["id"]), [])
        bucket.append({
            "name": c["name"],
            "package": c["package"],
            "rating": c["rating"],
            "downloads": c["downloads"],
            "pain_points": [{"issue": p.get("issue", ""),
                             "frequency": p.get("frequency", "")}
                            for p in pains[:pains_per_comp]],
        })

    for app in apps:
        app["competitors"] = comps_by_app.get(app["apple_id"], [])
        ratings = [c["rating"] for c in app["competitors"]
                   if isinstance(c["rating"], (int, float))]
        app["latest_probe"] = {
            "probed_at": app.get("last_probe_at") or "",
            "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else None,
        }
    return apps


def apps_for_filter(days: int = 60, db_path=None, limit: int = 300) -> list[dict]:
    """AI 筛选输入：最近 N 天出现的应用 + 最近一轮竞品与痛点摘要。

    保留给旧调用方；增量筛选请用 apps_needing_verdict()。
    只取最近一轮 probe 的竞品（最多 3 个）与痛点（每个最多 3 条），
    把 token 预算留给应用数量而不是历史冗余。
    """
    conn = connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT * FROM apps
            WHERE last_seen >= ?
            ORDER BY (last_probe_at IS NOT NULL) DESC, last_seen DESC
            LIMIT ?
            """,
            (_days_ago(days), int(limit)),
        ).fetchall()
        apps = [_app_row(r) for r in rows]
        return _attach_latest_competitors(conn, apps)
    finally:
        conn.close()


def apps_needing_verdict(db_path=None, decisions_db_path=None, limit: int = 300,
                         criteria_hash: str = "",
                         include_hash_change: bool = False,
                         now: str | None = None) -> list[dict]:
    """返回需要（重新）裁决的应用，按「标杆 > 最佳排名 > 最近出现」排序。

    需要裁决的判定：
      1. 从未有裁决；
      2. 最新裁决 basis='catalog_only'，但应用之后已被 probe（依据升级）；
      3. 最新裁决已过 TTL；
      4. 可选：提示词/模型哈希变化（include_hash_change=True）。
    只返回应用事实 + 最近一轮竞品摘要，不返回历史裁决全文。
    """
    raw_path = Path(db_path) if db_path else DEFAULT_DB_PATH
    dec_path = Path(decisions_db_path) if decisions_db_path else DEFAULT_DECISIONS_DB_PATH
    now = now or _now()
    conn = connect(raw_path)
    try:
        conn.execute("ATTACH DATABASE ? AS decisions", (str(dec_path),))
        clauses = [
            "v.id IS NULL",
            "v.expires_at IS NULL OR v.expires_at <= ?",
            "(v.basis='catalog_only' AND a.last_probe_at IS NOT NULL "
            " AND a.last_probe_at > v.filtered_at)",
        ]
        params: list = [now]
        if include_hash_change and criteria_hash:
            clauses.append("(v.criteria_hash IS NOT NULL AND v.criteria_hash <> '' "
                           "AND v.criteria_hash <> ?)")
            params.append(criteria_hash)
        sql = f"""
        SELECT a.* FROM apps a
        LEFT JOIN (
          SELECT apple_id, MAX(id) AS max_id
          FROM decisions.app_verdicts GROUP BY apple_id
        ) l ON l.apple_id = a.apple_id
        LEFT JOIN decisions.app_verdicts v ON v.id = l.max_id
        WHERE {' OR '.join('(' + c + ')' for c in clauses)}
        ORDER BY a.is_benchmark DESC,
                 CASE WHEN a.best_rank IS NULL THEN 1 ELSE 0 END,
                 a.best_rank ASC,
                 a.last_seen DESC
        LIMIT ?
        """
        params.append(int(limit))
        rows = conn.execute(sql, params).fetchall()
        apps = [_app_row(r) for r in rows]
        return _attach_latest_competitors(conn, apps)
    finally:
        conn.close()


def latest_accepted_verdicts(decisions_db_path=None, db_path=None) -> list[dict]:
    """返回每个应用最新一条裁决中 verdict='accept' 的记录（附带应用事实）。"""
    raw_path = Path(db_path) if db_path else DEFAULT_DB_PATH
    dec_path = Path(decisions_db_path) if decisions_db_path else DEFAULT_DECISIONS_DB_PATH
    conn = connect(raw_path)
    try:
        conn.execute("ATTACH DATABASE ? AS decisions", (str(dec_path),))
        rows = conn.execute(
            """
            SELECT v.*, a.name AS app_name, a.artist AS app_artist,
                   a.jtbd AS app_jtbd, a.url AS app_url, a.is_blank,
                   a.competitor_count, a.genre, a.avg_rating, a.rating_count,
                   a.best_rank, a.chart_hits, a.last_probe_at
            FROM decisions.app_verdicts v
            JOIN (
              SELECT apple_id, MAX(id) AS max_id
              FROM decisions.app_verdicts GROUP BY apple_id
            ) l ON l.max_id = v.id
            LEFT JOIN apps a ON a.apple_id = v.apple_id
            WHERE v.verdict='accept'
            ORDER BY v.score DESC, v.filtered_at DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def count_apps(db_path=None) -> int:
    conn = connect(db_path)
    try:
        return int(_scalar(conn, "SELECT COUNT(*) FROM apps"))
    finally:
        conn.close()


# ============================================================
# 写入：filter 裁决（decisions.db）
# ============================================================
VERDICT_FIELDS = (
    "apple_id", "verdict", "score", "reason", "attack_vector",
    "native_features", "competitors_summary", "basis", "app_metrics_version",
    "probe_snapshot_at", "competitor_count_snapshot", "criteria_hash",
    "model", "batch_id", "filtered_at", "expires_at",
)


def insert_verdicts(rows: list[dict], decisions_db_path=None) -> int:
    """批量写入裁决记录（含 reject），返回写入条数。"""
    if not rows:
        return 0
    path = Path(decisions_db_path) if decisions_db_path else DEFAULT_DECISIONS_DB_PATH
    conn = connect(path)
    try:
        columns = ", ".join(VERDICT_FIELDS)
        placeholders = ", ".join("?" for _ in VERDICT_FIELDS)
        conn.executemany(
            f"INSERT INTO app_verdicts ({columns}) VALUES ({placeholders})",
            [tuple(row.get(k) for k in VERDICT_FIELDS) for row in rows],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def start_filter_run(batch_id: str, model: str, criteria_hash: str,
                     input_count: int, cached_count: int,
                     decisions_db_path=None) -> int:
    """写入一条批次记录（finished_at 留空），返回 filter_runs.id。"""
    path = Path(decisions_db_path) if decisions_db_path else DEFAULT_DECISIONS_DB_PATH
    conn = connect(path)
    try:
        cur = conn.execute(
            """INSERT INTO filter_runs
               (batch_id, model, criteria_hash, input_count, accept_count,
                reject_count, cached_count, started_at)
               VALUES (?, ?, ?, ?, 0, 0, ?, ?)""",
            (batch_id, model, criteria_hash, int(input_count), int(cached_count),
             _now()),
        )
        conn.commit()
        return int(cur.lastrowid or 0)
    finally:
        conn.close()


def finish_filter_run(run_id: int, accept_count: int, reject_count: int,
                      decisions_db_path=None) -> None:
    if not run_id:
        return
    path = Path(decisions_db_path) if decisions_db_path else DEFAULT_DECISIONS_DB_PATH
    conn = connect(path)
    try:
        conn.execute(
            """UPDATE filter_runs SET accept_count=?, reject_count=?,
               finished_at=? WHERE id=?""",
            (int(accept_count), int(reject_count), _now(), int(run_id)),
        )
        conn.commit()
    finally:
        conn.close()


# ============================================================
# 灵感验证：纯事实查询（不算分，由外部 agent 自行推理）
# ============================================================
def validate_idea(idea_keywords: str | list[str], genre: str | None = None,
                  db_path=None, limit: int = 10) -> dict:
    """根据产品创意关键词查询 Apple 标杆 + 华为竞品的完整事实。

    返回结构化事实，不做任何评分或结论。外部 agent 可据此自行推理
    「是否有需求 / 竞品是否饱和 / 痛点在哪里」。
    """
    if isinstance(idea_keywords, str):
        keywords = [kw.strip() for kw in idea_keywords.split() if kw.strip()]
    else:
        keywords = [str(kw).strip() for kw in (idea_keywords or [])
                    if str(kw).strip()]

    query = " ".join(keywords)
    if not keywords:
        return {"query": query, "found_benchmarks": [],
                "error": "关键词不能为空"}

    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    if not path.exists():
        return {"query": query, "found_benchmarks": [],
                "message": f"数据库不存在: {path}，请先运行 capture"}
    init_db(path)

    keyword_clauses = []
    params: list = []
    for kw in keywords:
        keyword_clauses.append(
            "(a.name LIKE ? OR a.jtbd LIKE ? OR a.description LIKE ? "
            "OR a.search_keywords LIKE ?)")
        like = f"%{kw}%"
        params.extend([like, like, like, like])

    where = ["(" + " OR ".join(keyword_clauses) + ")"]
    if genre and str(genre).strip():
        where.append("a.genre LIKE ?")
        params.append(f"%{str(genre).strip()}%")

    conn = connect(path)
    try:
        rows = conn.execute(
            f"""
            SELECT a.* FROM apps a
            WHERE {' AND '.join(where)}
            ORDER BY a.is_benchmark DESC,
                     CASE WHEN a.best_rank IS NULL THEN 1 ELSE 0 END,
                     a.best_rank ASC,
                     a.last_seen DESC
            LIMIT ?
            """,
            params + [int(limit)],
        ).fetchall()
        apps = [_app_row(r) for r in rows]
    finally:
        conn.close()

    if not apps:
        return {"query": query, "found_benchmarks": [],
                "message": f"数据库中未找到与 '{query}' 相关的 Apple 应用"}

    benchmarks = []
    for app in apps:
        detail = get_app_detail(app["apple_id"], db_path=path)
        if not detail:
            continue
        latest_at = detail.get("last_probe_at") or ""
        latest_competitors = [
            c for c in detail.get("competitors", [])
            if latest_at and c.get("probed_at") == latest_at
        ]
        competitors = []
        for comp in latest_competitors[:5]:
            pains = comp.get("pain_points") or []
            competitors.append({
                "name": comp.get("name", ""),
                "package": comp.get("package", ""),
                "rating": comp.get("rating"),
                "downloads": comp.get("downloads", ""),
                "category": comp.get("category", ""),
                "intro": comp.get("intro", ""),
                "update_time": comp.get("update_time", ""),
                "high_freq_pains": [
                    {"issue": p.get("issue", ""),
                     "quote": str(p.get("quote") or "")[:100]}
                    for p in pains if p.get("frequency") == "高频"
                ][:3],
                "mid_freq_pains": [
                    {"issue": p.get("issue", ""),
                     "quote": str(p.get("quote") or "")[:100]}
                    for p in pains if p.get("frequency") == "中频"
                ][:2],
            })
        benchmarks.append({
            "apple": {
                "name": detail.get("name", ""),
                "apple_id": detail.get("apple_id", ""),
                "url": detail.get("url", ""),
                "artist": detail.get("artist", ""),
                "genre": detail.get("genre", ""),
                "description": detail.get("description", ""),
                "avg_rating": detail.get("avg_rating"),
                "rating_count": detail.get("rating_count"),
                "price": detail.get("price"),
                "release_date": detail.get("release_date", ""),
                "best_rank": detail.get("best_rank"),
                "chart_hits": detail.get("chart_hits") or [],
                "is_benchmark": bool(detail.get("is_benchmark")),
                "first_seen": detail.get("first_seen", ""),
                "last_seen": detail.get("last_seen", ""),
            },
            "huawei": {
                "is_blank": bool(detail.get("is_blank")),
                "probe_status": detail.get("probe_status") or "",
                "evidence_grade": detail.get("probe_evidence_grade") or "",
                "competitor_count": len(latest_competitors),
                "latest_probe_at": latest_at,
                "competitors": competitors,
            },
        })

    summary = {
        "total_benchmarks": len(benchmarks),
        "blank_count": sum(1 for b in benchmarks if b["huawei"]["is_blank"]),
        "high_pain_benchmarks": sum(
            1 for b in benchmarks
            if any(len(c.get("high_freq_pains", [])) >= 2
                   for c in b["huawei"]["competitors"])),
        "featured_count": 0,
    }
    return {"query": query, "found_benchmarks": benchmarks,
            "summary_stats": summary}


def export_idea_validation(idea_keywords: str | list[str],
                           output_path=None, genre: str | None = None,
                           db_path=None) -> str:
    """把 validate_idea 结果导出为 JSON 文件，返回文件路径。"""
    result = validate_idea(idea_keywords, genre=genre, db_path=db_path)
    if output_path is None:
        raw = (idea_keywords if isinstance(idea_keywords, list)
               else str(idea_keywords).split())
        safe_name = "_".join(str(k) for k in raw)[:50]
        safe_name = "".join(
            c for c in safe_name if c.isalnum() or c in (" ", "_", "-")).strip()
        output_path = DATA_DIR / "export" / f"idea_{safe_name or 'query'}.json"
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return str(path)


# ============================================================
# 导出 / 清理
# ============================================================
CSV_COLUMNS = [
    ("name", "应用名"),
    ("apple_id", "Apple ID"),
    ("artist", "开发者"),
    ("jtbd", "JTBD"),
    ("first_seen", "首次出现"),
    ("last_seen", "最近出现"),
    ("comp_total", "竞品数"),
    ("comp_avg_rating", "平均评分"),
    ("high_pain_count", "高频痛点"),
    ("mid_pain_count", "中频痛点"),
    ("is_blank", "生态空白"),
    ("needs_refresh", "待刷新"),
    ("probe_status", "Probe 状态"),
]


def csv_text(filters: dict | None = None, db_path=None) -> str:
    """把筛选结果渲染为 CSV 文本（UTF-8-SIG BOM 由调用方按需加）。"""
    apps = get_apps(filters, db_path=db_path)
    lines = [",".join(_csv_cell(label) for _, label in CSV_COLUMNS)]
    for app in apps:
        row = []
        for key, _ in CSV_COLUMNS:
            val = app.get(key)
            if isinstance(val, bool):
                val = "是" if val else "否"
            elif val is None:
                val = ""
            elif isinstance(val, float):
                val = f"{val:.2f}"
            row.append(_csv_cell(val))
        lines.append(",".join(row))
    return "\n".join(lines) + "\n"


def _csv_cell(val) -> str:
    text = str(val)
    if any(ch in text for ch in (',', '"', "\n", "\r")):
        return '"' + text.replace('"', '""') + '"'
    return text


def export_to_csv(filters: dict | None = None, output_path=None, db_path=None) -> str:
    """把筛选结果导出为 CSV 文件（UTF-8-SIG，Excel 可直接打开）。"""
    path = Path(output_path) if output_path else (DATA_DIR / "apps_export.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(csv_text(filters, db_path=db_path), encoding="utf-8-sig")
    return str(path)


def clean_old_apps(days: int = 90, db_path=None) -> int:
    """删除超过 N 天未出现的应用（级联删除竞品与差评），返回删除条数。"""
    if not days or int(days) <= 0:
        return 0
    cutoff = _days_ago(int(days))
    conn = connect(db_path)
    try:
        ids = [r[0] for r in conn.execute(
            "SELECT apple_id FROM apps WHERE last_seen < ?", (cutoff,)).fetchall()]
        for apple_id in ids:
            conn.execute("DELETE FROM reviews WHERE competitor_id IN"
                         " (SELECT id FROM competitors WHERE apple_id=?)", (apple_id,))
            conn.execute("DELETE FROM competitors WHERE apple_id=?", (apple_id,))
            conn.execute("DELETE FROM apps WHERE apple_id=?", (apple_id,))
        conn.commit()
        try:
            conn.execute("VACUUM")
        except sqlite3.Error:
            pass
        return len(ids)
    finally:
        conn.close()


def import_from_artifacts(benchmarks: dict | None, probe: dict | None,
                          db_path=None) -> dict:
    """把已有 JSON 产物回填进数据库（首次升级或迁移时用）。

    返回 {"apps": 入库数, "probed": 回填探测状态数}。
    """
    seen_at = str((benchmarks or {}).get("captured_at") or "") or _now()
    apps = 0
    for bm in (benchmarks or {}).get("benchmarks", []) or []:
        if upsert_app(bm, db_path=db_path, seen_at=seen_at, is_benchmark=True):
            apps += 1

    probed_at = str((probe or {}).get("probed_at") or "") or _now()
    probed = 0
    conn = connect(db_path)
    try:
        already = {r[0] for r in conn.execute(
            "SELECT DISTINCT apple_id FROM competitors WHERE probed_at=?",
            (probed_at,)).fetchall()}
    finally:
        conn.close()

    for record in (probe or {}).get("benchmarks", []) or []:
        apple_id = str(record.get("apple_id", "") or "").strip()
        if not apple_id:
            continue
        # 同一 probed_at 的竞品快照已回填过则跳过，保证命令可重复执行
        if apple_id not in already:
            # 历史 probe.json 没有 pain_points；差评原文无法结构化回填，
            # 这里只补状态与竞品事实，避免伪造痛点。
            add_competitors(apple_id, record.get("competitors") or [],
                            probed_at, db_path=db_path)
        if update_probe_state(apple_id, record, probed_at, db_path=db_path):
            probed += 1
    return {"apps": apps, "probed": probed}


def _to_float(val) -> float | None:
    if val is None or isinstance(val, bool):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
