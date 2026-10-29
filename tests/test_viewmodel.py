# -*- coding: utf-8 -*-
"""viewmodel 回归：数据库展示层派生、时间格式化、完整度判断。

仅测试不依赖 Streamlit 运行时的纯函数。
"""
import unittest

from radar import viewmodel as vm


def _app(**extra):
    base = {
        "name": "A", "apple_id": "1", "jtbd": "j",
        "first_seen": "2026-10-01T10:00:00+08:00",
        "last_seen": "2026-10-29T10:00:00+08:00",
        "comp_total": 2, "comp_avg_rating": 3.2,
        "high_pain_count": 1, "mid_pain_count": 2, "pain_count": 3,
        "last_probe_at": "2026-10-29T10:00:00+08:00",
        "probe_status": "confirmed_competitors", "is_blank": False,
        "needs_refresh": False, "is_favorite": False,
    }
    base.update(extra)
    return base


class DistinctPainTest(unittest.TestCase):
    def test_duplicate_pain_text_collapsed(self):
        pains = ["[1星] 广告太多", "[1星] 广告太多", "[1星] 会闪退"]
        self.assertEqual(vm.distinct_pain_count(pains), 2)

    def test_empty_pain_list(self):
        self.assertEqual(vm.distinct_pain_count(None), 0)
        self.assertEqual(vm.distinct_pain_count([]), 0)


class FormattingTest(unittest.TestCase):
    def test_fmt_ts_parses_iso(self):
        self.assertIn("2026-10-29", vm.fmt_ts("2026-10-29T10:00:00+08:00"))

    def test_fmt_ts_falls_back_on_garbage(self):
        self.assertEqual(vm.fmt_ts("not-a-date"), "not-a-date")

    def test_fmt_ts_empty_is_dash(self):
        self.assertEqual(vm.fmt_ts(""), "—")

    def test_days_since_unknown_returns_none(self):
        self.assertIsNone(vm.days_since(""))
        self.assertIsNone(vm.days_since("garbage"))

    def test_days_since_never_negative(self):
        self.assertGreaterEqual(vm.days_since("2026-10-30T00:00:00+08:00") or 0, 0)


class BucketTest(unittest.TestCase):
    def test_competitor_bucket(self):
        self.assertEqual(vm.comp_bucket(0), vm.COMP_NONE)
        self.assertEqual(vm.comp_bucket(1), vm.COMP_LOW)
        self.assertEqual(vm.comp_bucket(2), vm.COMP_LOW)
        self.assertEqual(vm.comp_bucket(3), vm.COMP_HIGH)

    def test_rating_label(self):
        self.assertEqual(vm.rating_label(None), "—")
        self.assertEqual(vm.rating_label(3.2), "3.2")

    def test_pain_label_priority(self):
        self.assertEqual(vm.pain_label({"high_pain_count": 2, "mid_pain_count": 5}),
                         "高频 2")
        self.assertEqual(vm.pain_label({"high_pain_count": 0, "mid_pain_count": 3}),
                         "中频 3")
        self.assertEqual(vm.pain_label({}), "—")


class StatusTagsTest(unittest.TestCase):
    def test_unprobed_tag(self):
        tags = vm.status_tags({"last_probe_at": None})
        self.assertIn("未探测", tags)
        self.assertNotIn("待刷新", tags)

    def test_blank_tag_only_after_probe(self):
        tags = vm.status_tags({"last_probe_at": "2026-01-01", "is_blank": True})
        self.assertIn("生态空白（待验证）", tags)

    def test_refresh_and_favorite(self):
        tags = vm.status_tags({"last_probe_at": "2026-01-01",
                               "needs_refresh": True, "is_favorite": True})
        self.assertIn("待刷新", tags)
        self.assertIn("已收藏", tags)


class AppRowTest(unittest.TestCase):
    def test_row_uses_derived_columns(self):
        row = vm.app_row(_app())
        self.assertEqual(row["competitor_count"], 2)
        self.assertEqual(row["competitor_bucket"], vm.COMP_LOW)
        self.assertEqual(row["avg_rating"], "3.2")
        self.assertEqual(row["pain"], "高频 1")

    def test_row_handles_missing_derived_columns(self):
        row = vm.app_row({"name": "B", "apple_id": "2", "competitor_count": 0})
        self.assertEqual(row["competitor_count"], 0)
        self.assertEqual(row["avg_rating"], "—")


class StatsCardsTest(unittest.TestCase):
    def test_cards_cover_key_metrics(self):
        cards = vm.stats_cards({"total": 10, "new_7": 3, "new_30": 8,
                                "stale": 4, "blank": 2, "with_competitors": 5,
                                "pain_apps": 6, "favorites": 1,
                                "refresh_days": 30})
        labels = [c["label"] for c in cards]
        self.assertIn("应用总数", labels)
        self.assertIn("待刷新", labels)
        self.assertIn("确认空白", labels)
        blank_card = [c for c in cards if c["label"] == "确认空白"][0]
        self.assertEqual(blank_card["tone"], "gap")


class FilterEchoTest(unittest.TestCase):
    def test_empty_filters_show_all(self):
        self.assertEqual(vm.filter_echo({}), "全部应用")

    def test_combined_filters(self):
        echo = vm.filter_echo({"days": 30, "competitor": "0", "pain": "high"})
        self.assertIn("最近 30 天", echo)
        self.assertIn("无竞品", echo)
        self.assertIn("有高频痛点", echo)


class CompletenessTest(unittest.TestCase):
    def test_db_with_probe_is_complete(self):
        rows = [{"file": "probe.json", "ok": True}]
        c = vm.completeness(rows, {"total": 5, "probed": 3})
        self.assertTrue(c["complete"])
        self.assertEqual(c["stage"], "probe")

    def test_db_without_probe_needs_probe(self):
        c = vm.completeness([], {"total": 5, "probed": 0})
        self.assertFalse(c["complete"])
        self.assertEqual(c["stage"], "capture")

    def test_empty_db(self):
        c = vm.completeness([], {"total": 0, "probed": 0})
        self.assertEqual(c["stage"], "none")

    def test_raw_only(self):
        rows = [{"file": "raw_feed.json", "ok": True}]
        c = vm.completeness(rows, {})
        self.assertEqual(c["stage"], "raw")


class ArtifactTest(unittest.TestCase):
    def test_load_artifacts_reports_corrupt(self):
        def reader(fn):
            if fn == "probe.json":
                return None, "数据文件损坏（probe.json）"
            return None, None
        out = vm.load_artifacts(reader)
        self.assertTrue(any("损坏" in e for e in out["errors"]))
        corrupt = [r for r in out["rows"] if r["file"] == "probe.json"][0]
        self.assertFalse(corrupt["ok"])

    def test_artifact_specs_have_no_audit_files(self):
        names = [spec[0] for spec in vm.ARTIFACT_SPECS]
        self.assertNotIn("audit_results.json", names)
        self.assertNotIn("opportunities.json", names)
        self.assertIn("filter_results.json", names)


class ParseKeywordsTest(unittest.TestCase):
    def test_json_string(self):
        self.assertEqual(vm.parse_search_keywords('["a","b"]'), ["a", "b"])

    def test_list_passthrough(self):
        self.assertEqual(vm.parse_search_keywords(["a"]), ["a"])

    def test_garbage_returns_empty(self):
        self.assertEqual(vm.parse_search_keywords("not json"), [])
        self.assertEqual(vm.parse_search_keywords(None), [])


if __name__ == "__main__":
    unittest.main()
