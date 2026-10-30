# -*- coding: utf-8 -*-
"""故障注入测试：网络/LLM/格式故障时保留历史产物，不写空覆盖。"""
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from radar.pipeline import PipelineAbortError, RetryableAbortError
from radar.pipeline import capture, filter as filter_stage, probe


class CaptureFailureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "apps.db")

    def tearDown(self):
        self.tmp.cleanup()

    def _cfg(self):
        return {"llm": {"model": "m", "base_url": "http://x"},
                "capture": {"regions": ["cn"], "charts": ["top-free"],
                            "feed_limit": 10, "max_benchmarks": 5,
                            "llm_input_limit": 200, "enrich_itunes": False},
                "database": {"path": self.db}}

    def test_all_apple_feeds_fail_is_retryable(self):
        with mock.patch.object(capture.apple_rss, "fetch_all", return_value=[]):
            with self.assertRaises(RetryableAbortError):
                capture.run(self._cfg())

    def test_llm_connection_failure_aborts_without_overwrite(self):
        apps = [{"name": "A", "artist": "X", "apple_id": "1", "rank": 1,
                 "url": "u", "source": "apple/cn/top-free"}]
        saved = []

        class BoomLLM:
            def __init__(self, *a, **k):
                pass

            def chat_json(self, *a, **k):
                from radar.llm import LLMError
                raise LLMError("down")

        with mock.patch.object(capture.apple_rss, "fetch_all", return_value=apps), \
                mock.patch.object(capture, "LLMClient", BoomLLM), \
                mock.patch.object(capture.store, "save_raw_feed",
                                  side_effect=lambda d: saved.append("raw")), \
                mock.patch.object(capture.store, "save_benchmarks",
                                  side_effect=lambda d: saved.append("bench")):
            with self.assertRaises(RetryableAbortError):
                capture.run(self._cfg())
        # 原始快照已落盘（便于重跑），但绝不写空 benchmarks
        self.assertIn("raw", saved)
        self.assertNotIn("bench", saved)
        # 目录层在 LLM 失败前已入库，避免白跑一轮网络抓取
        from radar import database
        self.assertIsNotNone(database.get_app_detail("1", self.db))

    def test_empty_llm_selection_triggers_abort(self):
        apps = [{"name": "A", "artist": "X", "apple_id": "1", "rank": 1,
                 "url": "u", "source": "apple/cn/top-free"}]

        class EmptyLLM:
            def __init__(self, *a, **k):
                pass

            def chat_json(self, *a, **k):
                return []

        with mock.patch.object(capture.apple_rss, "fetch_all", return_value=apps), \
                mock.patch.object(capture, "LLMClient", EmptyLLM), \
                mock.patch.object(capture.store, "save_raw_feed"), \
                mock.patch.object(capture.store, "save_benchmarks") as save:
            with self.assertRaises(PipelineAbortError):
                capture.run(self._cfg())
        save.assert_not_called()


class ProbeFailureTest(unittest.TestCase):
    def test_empty_benchmarks_aborts(self):
        with mock.patch.object(probe.store, "load_benchmarks", return_value=None):
            with self.assertRaises(PipelineAbortError):
                probe.run({"llm": {"model": "m", "base_url": "http://x"}, "probe": {}})

    def test_empty_results_abort_without_overwrite(self):
        class FailingClient:
            def search(self, *a, **k):
                from radar.sources.appgallery import AppGalleryError
                raise AppGalleryError("boom")

            def detail(self, *a, **k):
                return None

            def comments(self, *a, **k):
                return {"list": []}

            def close(self):
                pass

        bench = {"benchmarks": [{"name": "A", "apple_id": "1",
                                 "search_keywords": ["k"]}]}
        saved = []
        with mock.patch.object(probe.store, "load_benchmarks", return_value=bench), \
                mock.patch.object(probe, "AppGalleryClient", lambda: FailingClient()), \
                mock.patch.object(probe.store, "save_probe",
                                  side_effect=lambda d: saved.append(d)):
            probe.run({"llm": {"model": "m", "base_url": "http://x"},
                       "probe": {"enable_llm_filter": False}})
        # request_error 记录仍会产出，因此这里检查的是「有记录、非空」
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["benchmarks"][0]["probe_status"], "request_error")


class FilterFailureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "apps.db")
        self.dec = str(Path(self.tmp.name) / "decisions.db")

    def tearDown(self):
        self.tmp.cleanup()

    def _cfg(self):
        return {"llm": {"model": "m", "base_url": "http://x"},
                "database": {"path": self.db, "decisions_path": self.dec},
                "filter": {"max_apps": 300, "batch_size": 50,
                           "verdict_ttl_days": 90}}

    def test_no_pending_apps_returns_zero_without_llm(self):
        with mock.patch.object(filter_stage.database, "apps_needing_verdict",
                               return_value=[]):
            self.assertEqual(filter_stage.run(self._cfg()), 0)

    def test_llm_failure_is_retryable(self):
        from radar.llm import LLMError

        class BoomLLM:
            def __init__(self, *a, **k):
                pass

            def chat_json(self, *a, **k):
                raise LLMError("down")

        with mock.patch.object(
                filter_stage.database, "apps_needing_verdict",
                return_value=[{"apple_id": "1", "name": "A",
                               "metrics_version": 1}]), \
                mock.patch.object(filter_stage, "LLMClient", BoomLLM), \
                mock.patch.object(filter_stage.store, "save_filter_results"):
            with self.assertRaises(RetryableAbortError):
                filter_stage.run(self._cfg())

    def test_empty_result_does_not_overwrite(self):
        class EmptyLLM:
            def __init__(self, *a, **k):
                pass

            def chat_json(self, *a, **k):
                return []

        saved = []
        with mock.patch.object(
                filter_stage.database, "apps_needing_verdict",
                return_value=[{"apple_id": "1", "name": "A",
                               "metrics_version": 1}]), \
                mock.patch.object(filter_stage, "LLMClient", EmptyLLM), \
                mock.patch.object(filter_stage.store, "save_filter_results",
                                  side_effect=lambda d: saved.append(d)):
            with self.assertRaises(PipelineAbortError):
                filter_stage.run(self._cfg())
        self.assertEqual(saved, [])


class CliExitCodeTest(unittest.TestCase):
    """退出码约定：可重试中断 = 2，致命熔断 = 1，成功 = 0。"""

    def _main_rc(self, run_stage_rc, command="capture"):
        from radar import cli
        with mock.patch.object(cli, "load_config", return_value={}), \
                mock.patch.object(cli, "_run_stage", return_value=run_stage_rc):
            return cli.main([command])

    def test_success_returns_zero(self):
        self.assertEqual(self._main_rc(5), 0)

    def test_fatal_abort_returns_one(self):
        self.assertEqual(self._main_rc(-1), 1)

    def test_retryable_abort_returns_two(self):
        self.assertEqual(self._main_rc(-2), 2)


class CliCommandTest(unittest.TestCase):
    def test_audit_command_removed(self):
        import contextlib
        import io
        from radar import cli
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                cli._build_parser().parse_args(["audit"])

    def test_filter_command_exists(self):
        from radar import cli
        args = cli._build_parser().parse_args(["filter"])
        self.assertEqual(args.command, "filter")

    def test_database_subcommands(self):
        from radar import cli
        for sub in ("init", "stats", "refresh", "export"):
            args = cli._build_parser().parse_args(["database", sub])
            self.assertEqual(args.db_command, sub)


if __name__ == "__main__":
    unittest.main()
