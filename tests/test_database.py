# -*- coding: utf-8 -*-
"""database 单元测试：增量写入、历史快照、筛选与统计。"""
import tempfile
import unittest
from pathlib import Path

from radar import database


def _bm(apple_id="1", name="A", **extra):
    base = {"apple_id": apple_id, "name": name, "artist": "X", "jtbd": "当我X时",
            "pick_reason": "R", "search_keywords": ["k1", "k2"],
            "url": "u", "source_charts": "apple/cn/top-free"}
    base.update(extra)
    return base


def _probe(apple_id="1", name="A", status="confirmed_competitors",
           grade="A", blank=False, competitors=None):
    comps = competitors or []
    return {"name": name, "apple_id": apple_id, "probe_status": status,
            "evidence_grade": grade, "blank_signal": blank,
            "competitor_count": len(comps), "competitors": comps}


def _comp(pkg="pkg.a", rating=4.5, pains=None, bad=0):
    return {"name": "C", "package": pkg, "category": "工具", "intro": "",
            "downloads": "10万次安装", "score": rating, "version": "1.0",
            "update_time": "", "review_evidence": "sufficient",
            "bad_review_count": bad, "pain_points": pains or []}


class DatabaseTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "apps.db")
        database.init_db(self.db)

    def tearDown(self):
        self.tmp.cleanup()


class UpsertTest(DatabaseTestCase):
    def test_new_app_inserted_with_defaults(self):
        aid = database.upsert_app(_bm(), db_path=self.db)
        self.assertEqual(aid, "1")
        app = database.get_app_detail("1", db_path=self.db)
        self.assertEqual(app["name"], "A")
        self.assertEqual(app["capture_count"], 1)
        self.assertFalse(app["is_blank"])
        self.assertIsNone(app["last_probe_at"])

    def test_second_capture_increments_count_and_keeps_probe_facts(self):
        database.upsert_app(_bm(), db_path=self.db)
        database.save_probe_record(_probe(), "2026-01-01T00:00:00+08:00",
                                   db_path=self.db)
        database.upsert_app(_bm(name="A2"), db_path=self.db)
        app = database.get_app_detail("1", db_path=self.db)
        self.assertEqual(app["capture_count"], 2)
        self.assertEqual(app["name"], "A2")
        self.assertEqual(app["last_probe_at"], "2026-01-01T00:00:00+08:00")

    def test_app_without_apple_id_is_skipped(self):
        self.assertIsNone(database.upsert_app({"name": "NoID"}, db_path=self.db))
        self.assertEqual(database.get_stats(self.db)["total"], 0)


class CompetitorHistoryTest(DatabaseTestCase):
    def test_two_probe_rounds_keep_both_snapshots(self):
        database.upsert_app(_bm(), db_path=self.db)
        database.save_probe_record(
            _probe(competitors=[_comp("pkg.a", 4.5)]),
            "2026-01-01T00:00:00+08:00", db_path=self.db)
        database.save_probe_record(
            _probe(competitors=[_comp("pkg.a", 3.0), _comp("pkg.b", 4.0)]),
            "2026-02-01T00:00:00+08:00", db_path=self.db)

        detail = database.get_app_detail("1", db_path=self.db)
        self.assertEqual(len(detail["competitors"]), 3)
        self.assertEqual(len(detail["rounds"]), 2)
        newest = detail["rounds"][0]
        self.assertEqual(newest["competitor_count"], 2)
        self.assertAlmostEqual(newest["avg_rating"], 3.5, places=2)

    def test_same_package_in_one_round_deduplicated(self):
        database.upsert_app(_bm(), db_path=self.db)
        ids = database.add_competitors(
            "1", [_comp("pkg.a"), _comp("pkg.a")],
            "2026-01-01T00:00:00+08:00", db_path=self.db)
        self.assertEqual(len(ids), 1)

    def test_pain_points_persisted_with_frequency(self):
        database.upsert_app(_bm(), db_path=self.db)
        pains = [{"issue": "广告太多", "frequency": "高频", "quote": "全是广告"},
                 {"issue": "闪退", "frequency": "中频", "quote": "一点就退"}]
        database.save_probe_record(
            _probe(competitors=[_comp(pains=pains)]),
            "2026-01-01T00:00:00+08:00", db_path=self.db)
        detail = database.get_app_detail("1", db_path=self.db)
        points = detail["competitors"][0]["pain_points"]
        self.assertEqual(len(points), 2)
        # 高频排在前面
        self.assertEqual(points[0]["frequency"], "高频")
        stats = database.get_stats(self.db)
        self.assertEqual(stats["review_rows"], 2)

    def test_blank_state_recorded(self):
        database.upsert_app(_bm(), db_path=self.db)
        database.save_probe_record(
            _probe(status="confirmed_empty", grade="C", blank=True),
            "2026-01-01T00:00:00+08:00", db_path=self.db)
        app = database.get_app_detail("1", db_path=self.db)
        self.assertTrue(app["is_blank"])
        self.assertEqual(app["probe_status"], "confirmed_empty")
        self.assertEqual(database.get_stats(self.db)["blank"], 1)


class FilterQueryTest(DatabaseTestCase):
    def _seed(self):
        database.upsert_app(_bm("1", "空白应用"), db_path=self.db)
        database.save_probe_record(
            _probe("1", "空白应用", status="confirmed_empty", grade="C", blank=True),
            "2026-01-01T00:00:00+08:00", db_path=self.db)

        database.upsert_app(_bm("2", "低分竞品应用"), db_path=self.db)
        database.save_probe_record(
            _probe("2", "低分竞品应用",
                   competitors=[_comp("pkg.b", 2.5,
                                      pains=[{"issue": "卡顿", "frequency": "高频"}])]),
            "2026-01-01T00:00:00+08:00", db_path=self.db)

        database.upsert_app(_bm("3", "未探测应用"), db_path=self.db)

    def test_competitor_filters(self):
        self._seed()
        zero = sorted(a["apple_id"] for a in
                      database.get_apps({"competitor": "0"}, db_path=self.db))
        self.assertEqual(zero, ["1", "3"])
        self.assertEqual([a["apple_id"] for a in
                          database.get_apps({"competitor": "1-2"}, db_path=self.db)],
                         ["2"])

    def test_rating_filter(self):
        self._seed()
        self.assertEqual([a["apple_id"] for a in
                          database.get_apps({"rating": "low"}, db_path=self.db)],
                         ["2"])
        self.assertEqual(len(database.get_apps({"rating": "none"}, db_path=self.db)), 2)

    def test_pain_filter(self):
        self._seed()
        rows = database.get_apps({"pain": "high"}, db_path=self.db)
        self.assertEqual([a["apple_id"] for a in rows], ["2"])

    def test_refresh_filter_and_mark(self):
        self._seed()
        marked = database.mark_needs_refresh(30, db_path=self.db)
        self.assertGreaterEqual(marked, 3)
        stale = database.get_apps({"refresh": "stale"}, db_path=self.db)
        self.assertEqual(len(stale), 3)

    def test_query_search(self):
        self._seed()
        rows = database.get_apps({"query": "低分"}, db_path=self.db)
        self.assertEqual(len(rows), 1)

    def test_derived_columns_present(self):
        self._seed()
        rows = database.get_apps({}, db_path=self.db)
        by_id = {r["apple_id"]: r for r in rows}
        self.assertEqual(by_id["2"]["comp_total"], 1)
        self.assertAlmostEqual(by_id["2"]["comp_avg_rating"], 2.5, places=2)
        self.assertEqual(by_id["2"]["high_pain_count"], 1)

    def test_favorite_toggle(self):
        self._seed()
        self.assertTrue(database.toggle_favorite("2", db_path=self.db))
        self.assertEqual(len(database.get_apps({"favorite": True}, db_path=self.db)), 1)
        self.assertFalse(database.toggle_favorite("2", db_path=self.db))


class ExportCleanTest(DatabaseTestCase):
    def test_export_csv_has_header_and_rows(self):
        database.upsert_app(_bm("1", "A"), db_path=self.db)
        database.save_probe_record(_probe(), "2026-01-01T00:00:00+08:00",
                                   db_path=self.db)
        out = Path(self.tmp.name) / "out.csv"
        path = database.export_to_csv({}, output_path=out, db_path=self.db)
        text = Path(path).read_text(encoding="utf-8-sig")
        self.assertIn("应用名", text)
        self.assertIn("A", text)

    def test_clean_old_apps_cascades(self):
        database.upsert_app(_bm("1"), db_path=self.db)
        database.save_probe_record(_probe(competitors=[_comp()]),
                                   "2026-01-01T00:00:00+08:00", db_path=self.db)
        # 手工把 last_seen 推到很久以前
        conn = database.connect(self.db)
        conn.execute("UPDATE apps SET last_seen='2020-01-01T00:00:00+08:00'")
        conn.commit()
        conn.close()
        removed = database.clean_old_apps(90, db_path=self.db)
        self.assertEqual(removed, 1)
        stats = database.get_stats(self.db)
        self.assertEqual(stats["total"], 0)
        self.assertEqual(stats["competitor_rows"], 0)

    def test_clean_disabled_when_days_zero(self):
        database.upsert_app(_bm("1"), db_path=self.db)
        self.assertEqual(database.clean_old_apps(0, db_path=self.db), 0)
        self.assertEqual(database.get_stats(self.db)["total"], 1)


class StatsTest(DatabaseTestCase):
    def test_stats_shape(self):
        database.upsert_app(_bm("1"), db_path=self.db)
        stats = database.get_stats(self.db)
        for key in ("total", "new_7", "new_30", "stale", "blank",
                    "with_competitors", "probed", "unprobed", "pain_apps",
                    "favorites", "competitor_rows", "review_rows"):
            self.assertIn(key, stats)
        self.assertEqual(stats["total"], 1)
        self.assertEqual(stats["unprobed"], 1)


if __name__ == "__main__":
    unittest.main()
