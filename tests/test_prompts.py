# -*- coding: utf-8 -*-
"""prompt 契约测试：占位符齐全、可安全格式化、关键规则与事实纪律。

内建默认模板与外部 prompts/*.txt 必须保持一致（同一占位符集合）。
"""
import unittest

from radar import prompts


class PromptContractTest(unittest.TestCase):
    def test_all_templates_validate_with_fixture_values(self):
        for name in prompts.PROMPT_TEMPLATES:
            text = prompts.get_prompt(name)
            errors = prompts.validate_prompt(name, text)
            self.assertEqual(errors, [], msg=f"{name}: {errors}")

    def test_required_placeholders_present_in_every_template(self):
        for name, spec in prompts.PROMPT_TEMPLATES.items():
            text = prompts.get_prompt(name)
            found = prompts.find_placeholders(text)
            for ph in spec["placeholders"]:
                self.assertIn(ph, found, msg=f"{name} 缺少 {ph}")

    def test_audit_templates_removed(self):
        self.assertNotIn("audit_system", prompts.PROMPT_TEMPLATES)
        self.assertNotIn("audit_user", prompts.PROMPT_TEMPLATES)

    def test_filter_template_has_required_placeholders(self):
        text = prompts.get_prompt("user_filter")
        self.assertIn("{database_json}", text)
        self.assertIn("{max_results}", text)

    def test_filter_system_enforces_fact_grounding(self):
        system = prompts.get_prompt("user_filter_system")
        self.assertIn("禁止虚构", system)
        self.assertIn("待验证", system)
        self.assertIn("JSON", system)

    def test_probe_filter_user_has_max_competitors(self):
        text = prompts.get_prompt("probe_filter_user")
        self.assertIn("{max_competitors}", text)

    def test_probe_filter_marks_empty_array(self):
        probe_filter = prompts.get_prompt("probe_filter_system")
        self.assertIn("空数组", probe_filter)

    def test_external_templates_match_builtin_placeholders(self):
        for name, spec in prompts.PROMPT_TEMPLATES.items():
            path = prompts.prompt_path(name)
            if not path.exists():
                continue
            external = path.read_text(encoding="utf-8")
            found = prompts.find_placeholders(external)
            builtin = prompts.find_placeholders(spec["default"])
            self.assertEqual(found, builtin,
                             msg=f"{name} 外部模板占位符与内建不一致")

    def test_pain_prompt_requires_verbatim_quote(self):
        system = prompts.get_prompt("probe_pain_system")
        self.assertIn("连续原文", system)
        self.assertIn("高频", system)


if __name__ == "__main__":
    unittest.main()
