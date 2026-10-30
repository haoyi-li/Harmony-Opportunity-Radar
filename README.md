# 纯血鸿蒙机会雷达 (Harmony Opportunity Radar)

以 Apple App Store 榜单为入口，把「Apple 已验证、鸿蒙侧可能存在供给落差」的应用逐条累积进本地数据库，再用按需 AI 筛选挑出值得做的方向。

> 定位说明：本工具产出的是**待验证的机会假设**，而非已证实的全市场结论。所有判断基于抓取到的事实，不虚构下载量、收入、评分趋势或平台能力。

## 设计原则

- **不做预先打分**：抓到的应用全部保留，不设分数门槛，不把「有机会的」提前过滤掉。
- **只增不减的本地资产**：每次抓取都写入 SQLite，老应用刷新时间戳，竞品与差评按轮次追加，可以看到趋势。
- **按需 AI 筛选**：想看机会时才调用 LLM，用你自己维护的提示词从全量数据里挑。

## 功能概览

| 阶段 | 命令 | 作用 |
|---|---|---|
| capture | `python -m radar capture` | 拉取 Apple 榜单 → LLM 精选标杆 → 提炼 JTBD → 写入数据库 |
| probe | `python -m radar probe` | 检索鸿蒙同类供给，两阶段过滤竞品，抓取差评并提炼痛点 → 写入数据库 |
| filter | `python -m radar filter` | 把数据库全量事实交给 AI，按 `prompts/user_filter.txt` 挑机会 |

每个阶段都会在 `data/runs/<batch_id>/` 下新建批次目录，归档中间产物、提示词快照与 LLM traces，互不覆盖。

## 环境要求

- Python 3.11+（依赖标准库 `tomllib` 与 `sqlite3`）
- Windows 环境可使用 `run.ps1` / `run.bat`；其他平台可直接使用 `python -m radar`

## 安装

```powershell
.\run.ps1 install
# 或
python -m pip install -r requirements.txt
```

## 配置

复制配置模板并按需修改：

```powershell
Copy-Item config.example.toml config.toml
```

`config.toml` 含真实 API Key，**已被 `.gitignore` 排除，禁止入库**。LLM 接口遵循 OpenAI 兼容协议，支持本地模型（vLLM / Ollama）与商用中转自由切换。

关键配置项：

- `[llm] base_url / api_key / model / timeout`
- `[capture] regions / charts / feed_limit / max_benchmarks / llm_input_limit / enrich_itunes`
- `[probe] max_comments_per_app / bad_rating_threshold / max_competitors`
- `[database] path / decisions_path / refresh_threshold_days / auto_clean_days`
- `[filter] max_apps / batch_size / verdict_ttl_days / enable_criteria_hash`

## 运行

```powershell
.\run.ps1              # 交互菜单
.\run.ps1 gui          # 启动 Streamlit 数据看板
.\run.ps1 full         # 全量流水线 capture -> probe
.\run.ps1 capture      # 仅捕获
.\run.ps1 probe        # 仅探测
.\run.ps1 filter       # AI 事后筛选
.\run.ps1 db-stats     # 数据库统计
```

也可直接使用 CLI：

```powershell
python -m radar capture --batch-id 20260101_120000
python -m radar probe   --batch-id 20260101_120000
python -m radar probe   --only-stale            # 只刷新超期未探测的应用
python -m radar probe   --apple-ids 123456,789  # 只刷新指定应用
python -m radar filter
```

单步执行默认继承最近一次成功批次的上游产物，也可用 `--base-run-id` 指定来源批次。

数据库子命令：

```powershell
python -m radar database init      # 初始化（幂等）
python -m radar database stats     # 统计概览
python -m radar database refresh   # 列出待刷新应用
python -m radar database export    # 导出 CSV
python -m radar database validate --idea "Markdown 笔记" --json  # 验证产品创意
python -m radar database clean --days 90
```

## 数据与产物

所有中间产物位于 `data/` 下，写入采用临时文件 + `os.replace` 原子替换。`data/` 已被 `.gitignore` 排除。

- `data/apps.db`：**原始仓库（核心资产）**，`apps` / `app_snapshots` / `competitors` / `reviews`，WAL 模式
- `data/decisions.db`：**筛选产物库**，`app_verdicts` / `filter_runs` / `knowledge_patterns`
- `data/runs/<batch_id>/`：批次隔离目录
- `data/runs/<batch_id>/llm_traces.jsonl`：结构化 LLM 调用记录
- `data/benchmarks.json`、`data/probe.json`：最近一次批次的镜像产物
- `data/filter_results.json`：累计最新 accept 结果（供 WebUI 与导出）

数据库表结构要点：

| 表 | 内容 |
|---|---|
| `apps` | 目录层全量应用 + iTunes 详情 + 榜单覆盖/最佳排名 + 标杆标记 |
| `app_snapshots` | 评分 / 评论数 / 排名变化快照（仅变化时追加） |
| `competitors` | 每轮 probe 的竞品快照（评分、下载、评论证据、`probed_at`），按轮次追加 |
| `reviews` | LLM 提炼的差评痛点（issue / frequency / quote），关联到竞品 |
| `app_verdicts` | 逐应用 accept/reject 裁决（含依据快照、模型、批次、TTL） |
| `filter_runs` | 每次筛选的输入 / 入选 / 拒绝 / 缓存命中计数 |

## WebUI

`.\run.ps1 gui` 启动后共有五个标签：

1. **数据库**：概览卡片、多条件筛选、应用列表、详情（竞品/痛点/Probe 历史）、刷新与导出
2. **AI 助手**：编辑筛选提示词 → 执行筛选 → 查看候选机会（可收藏、导出 Markdown）
3. **流水线**：捕获 / 探测 / AI 筛选按钮、产物状态、结构化 traces 与纯文本日志、配置表单
4. **历史批次**：回溯任意批次的产物与 LLM 调用
5. **提示词微调**：逐个编辑 `prompts/*.txt`

## 提示词

提示词模板独立存放于 `prompts/*.txt`，WebUI 可编辑并一键恢复默认。AI 筛选提示词（`prompts/user_filter.txt`）决定从全量数据库里挑什么，建议按当前想做的方向随时调整。

## 安全与隐私

- `config.toml`（含 API Key）与 `data/`（运行产物）均已排除入库
- 提交前请确认暂存区无密钥泄漏

## 项目结构

```
app.py                # Streamlit WebUI
radar/
  cli.py              # CLI 入口
  config.py           # 配置加载与校验
  database.py         # SQLite 增量数据库
  llm.py              # OpenAI 兼容 LLM 客户端
  store.py            # 批次归档与原子写入
  telemetry.py        # 结构化遥测事件
  viewmodel.py        # 数据库/产物的展示层读模型
  pipeline/           # capture / probe / filter
  sources/            # apple_rss / appgallery 数据源
  prompts/            # 提示词加载与内建默认模板
prompts/              # 外置提示词模板（可微调）
poc/                  # 早期接口探测与验证脚本（非正式入口）
```

## 依赖

仅依赖 Python 标准库（含 `sqlite3`）与 `httpx`、`streamlit`，不引入数据库服务、MQ 或 ORM。
