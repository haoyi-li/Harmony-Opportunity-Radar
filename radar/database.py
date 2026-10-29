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
  is_favorite INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_apps_apple_id ON apps(apple_id);
CREATE INDEX IF NOT EXISTS idx_apps_last_seen ON apps(last_seen);
CREATE INDEX IF NOT EXISTS idx_apps_needs_refresh ON apps(needs_refresh);

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
    """初始化数据库：创建表与索引（幂等）。"""
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
    return path


# ============================================================
# 写入：capture
# ============================================================
def upsert_app(benchmark: dict, db_path=None, seen_at: str | None = None) -> str | None:
    """插入或更新 apps 表，返回 apple_id（无 apple_id 时返回 None，不入库）。

    已存在的应用只刷新 last_seen / capture_count 与可能变动的文本字段，
    不重置 last_probe_at / is_blank / competitor_count 等 probe 事实。
    """
    apple_id = str((benchmark or {}).get("apple_id", "") or "").strip()
    name = str((benchmark or {}).get("name", "") or "").strip()
    if not apple_id or not name:
        return None
    now = seen_at or _now()
    keywords = benchmark.get("search_keywords") or []
    if not isinstance(keywords, list):
        keywords = []
    conn = connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO apps (apple_id, name, artist, jtbd, pick_reason, search_keywords,
                              url, source_charts, first_seen, last_seen, capture_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(apple_id) DO UPDATE SET
              name=excluded.name,
              artist=excluded.artist,
              jtbd=excluded.jtbd,
              pick_reason=excluded.pick_reason,
              search_keywords=excluded.search_keywords,
              url=excluded.url,
              source_charts=excluded.source_charts,
              last_seen=excluded.last_seen,
              capture_count=apps.capture_count + 1
            """,
            (
                apple_id, name,
                benchmark.get("artist", ""),
                benchmark.get("jtbd", ""),
                benchmark.get("pick_reason", ""),
                json.dumps(keywords, ensure_ascii=False),
                benchmark.get("url", ""),
                benchmark.get("source_charts", ""),
                now, now,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return apple_id


def upsert_benchmarks(benchmarks: list[dict], db_path=None,
                      seen_at: str | None = None) -> int:
    """批量写入 capture 精选结果，返回成功入库条数。"""
    now = seen_at or _now()
    count = 0
    for bm in benchmarks or []:
        if upsert_app(bm, db_path=db_path, seen_at=now):
            count += 1
    return count


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
    data["is_blank"] = bool(data.get("is_blank"))
    data["needs_refresh"] = bool(data.get("needs_refresh"))
    data["is_favorite"] = bool(data.get("is_favorite"))
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


def apps_for_filter(days: int = 60, db_path=None, limit: int = 300) -> list[dict]:
    """AI 筛选输入：最近 N 天出现的应用 + 最近一轮竞品与痛点摘要。

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
        if not apps:
            return []
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
    finally:
        conn.close()

    pains_by_comp: dict[int, list[dict]] = {}
    for r in rev_rows:
        pains_by_comp.setdefault(int(r["competitor_id"]), []).append(dict(r))

    comps_by_app: dict[str, list[dict]] = {}
    for c in comp_rows:
        bucket = comps_by_app.setdefault(c["apple_id"], [])
        if len(bucket) >= 3:
            continue
        pains = pains_by_comp.get(int(c["id"]), [])
        bucket.append({
            "name": c["name"],
            "package": c["package"],
            "rating": c["rating"],
            "downloads": c["downloads"],
            "pain_points": [{"issue": p.get("issue", ""),
                             "frequency": p.get("frequency", "")}
                            for p in pains[:3]],
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
        if upsert_app(bm, db_path=db_path, seen_at=seen_at):
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
