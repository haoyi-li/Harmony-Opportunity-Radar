# -*- coding: utf-8 -*-
"""store 单元测试：批次产物注册、质量统计与审计产物下线。"""
import unittest

from radar import store


class StoreArtifactTest(unittest.TestCase):
    def test_pipeline_artifacts_registered(self):
        self.assertIn("benchmarks.json", store.ALL_ARTIFACTS)
        self.assertIn("probe.json", store.ALL_ARTIFACTS)
        self.assertIn("filter_results.json", store.ALL_ARTIFACTS)

    def test_audit_artifacts_removed(self):
        for name in ("audit_results.json", "opportunities.json",
                     "review_queue.json"):
            self.assertNotIn(name, store.ALL_ARTIFACTS)
        self.assertNotIn("audit", store.STAGE_OUTPUTS)
        self.assertNotIn("audit", store.STAGE_INPUTS)

    def test_filter_stage_io(self):
        self.assertEqual(store.STAGE_OUTPUTS["filter"], ["filter_results.json"])
        # AI 筛选只读数据库，不继承 JSON 产物
        self.assertEqual(store.STAGE_INPUTS["filter"], [])

    def test_filter_readers_exist(self):
        self.assertTrue(callable(store.save_filter_results))
        self.assertTrue(callable(store.load_filter_results))
        self.assertFalse(hasattr(store, "save_audit_results"))
        self.assertFalse(hasattr(store, "save_opportunities"))


class BatchQualityTest(unittest.TestCase):
    def _probe_records(self):
        return [
            {"probe_status": "request_error"},
            {"probe_status": "llm_filter_failed"},
            {"probe_status": "weak_evidence"},
            {"probe_status": "confirmed_empty"},
            {"probe_status": "confirmed_competitors"},
        ]

    def test_quality_summary_counts_statuses(self):
        summary = store.summarize_quality(
            self._probe_records(), failed=1, invalid=2)
        self.assertEqual(summary["request_errors"], 1)
        self.assertEqual(summary["llm_filter_failures"], 1)
        self.assertEqual(summary["weak_evidence"], 1)
        self.assertEqual(summary["confirmed_empty"], 1)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["invalid"], 2)

    def test_quality_summary_excludes_api_key(self):
        summary = store.summarize_quality([], failed=0, invalid=0)
        self.assertNotIn("api_key", summary)


if __name__ == "__main__":
    unittest.main()
