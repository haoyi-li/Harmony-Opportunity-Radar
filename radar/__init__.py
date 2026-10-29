# -*- coding: utf-8 -*-
"""鸿蒙机会雷达（Harmony Opportunity Radar）主包。

流水线（对应 CLI 子命令）:
  capture  Apple 标杆捕获: 拉榜单 → LLM 精选 → JTBD 抽象 → data/benchmarks.json + 数据库
  probe    鸿蒙供给探测: 检索 AppGallery → 详情/评论抓取 → 痛点提炼 → data/probe.json + 数据库
  filter   AI 事后筛选: 数据库全量事实 → 按用户提示词挑机会（不做固定打分）
"""
