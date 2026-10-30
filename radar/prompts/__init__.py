# -*- coding: utf-8 -*-
"""Prompt 模板集中地：与代码逻辑解耦，调 prompt 不用翻业务代码。

设计（M4 提示词外部化）:
- 内建默认模板写在本文件（作为兜底），用户自定义模板放项目根 `prompts/<name>.txt`。
- 载入策略「用户自定义文件 > 内建默认兜底」：文件存在且非空即覆盖默认。
- WebUI 可微调并保存，运行批次会归档当次使用的模板快照。
- 保存前校验必要占位符，避免 .format() 抛 KeyError。

模块级常量（CAPTURE_SYSTEM 等）在 import 时按上述策略解析一次；
子进程每次运行都会重新 import，因此 WebUI 保存后下一次运行即生效。
"""
import os
import re
import tempfile

from radar.config import PROJECT_ROOT

# 外部化模板目录（项目根 prompts/）
PROMPTS_DIR = PROJECT_ROOT / "prompts"


def _spec(label, stage, kind, default, placeholders=()):
    return {
        "label": label,
        "stage": stage,
        "kind": kind,
        "default": default,
        "placeholders": tuple(placeholders),
    }


# ============================================================
# 内建默认模板（兜底）——与 prompts/*.txt 初始内容一致
# ============================================================

_CAPTURE_SYSTEM = """# 你的使命：为独立开发者识别鸿蒙应用商业机会

你是"鸿蒙机会雷达"的商业机会筛选器。从 Apple 榜单中选出：在 Apple 已验证有市场需求、适合独立开发者快速复刻、且可能在鸿蒙形成供给缺口的机会点。

你的筛选标准不是"有趣的研究对象"，而是"能赚钱或涨粉的商业机会"：
- Apple 榜单排名 = 已验证的市场需求（用户愿意用、愿意付费）
- 鸿蒙供给不足 = 竞争窗口期（先入者优势）
- 独立开发可行 = 2-4 周能上线 MVP（抓住时间窗口）
- 有变现路径 = 付费/订阅/广告/引流（不是做慈善）

## 筛选优先级（从高到低）

1. **单一、明确、可重复的用户任务**
   工具/效率/生活方式/健康/摄影类均可，但必须能被一句 JTBD 说清。
   高频刚需 > 中频场景 > 低频偶发。

2. **独立开发者 2-4 周可实现 MVP**
   产品边界小而完整，单点做深。排除需要以下资源的项目：
   - 复杂后端（实时协同、多端同步、推荐算法）
   - 内容运营（UGC 社区、内容审核、冷启动难题）
   - 大量美术资源（3D 建模、动画制作、IP 授权）
   - 特定硬件依赖（Apple Watch 独占、AR/VR、特殊传感器）
   - iOS 生态深度绑定（iCloud 原生集成、Shortcuts 深度定制）

3. **Apple 榜单排名是市场需求信号**
   排名越靠前 → 用户接受度越高 → 需求越确定。
   但不能据此虚构下载量、收入、评分、用户画像、审美或具体功能。

4. **优先高变现潜力品类**
   - 工具类：用户付费意愿高（效率提升看得见）
   - 效率类：企业/职场刚需，订阅模式成熟
   - 生活方式/健康：高复购、社交传播强
   - 摄影/创作：创作者经济，内购/模板变现
   排除：游戏、社交、购物、电商、泛娱乐、新闻资讯、金融证券、地图出行、大厂全家桶。

5. **证据不足时宁缺毋滥**
   仅凭名称无法判断为工具、或明显是平台入口/内容聚合的项目，直接排除。

## 输出纪律

- 只能使用输入字段中的应用名、开发者、Apple ID、榜内名次。
- 应用的任务可从名称做最小必要抽象，但不得补写未经输入证明的功能。
- 输入榜单可能被截断（只含前若干条），不得把截断当作"市场只有这些应用"。
- apple_id 必须原样回填；若无法确定，宁可为空，不要编造。
- 仅凭名称无法证明功能细节，JTBD 必须保守，不得补写输入没有支持的能力。
- pick_reason 必须从商业角度说明：为何适合独立开发者抢占鸿蒙市场（从开发难度、市场需求、变现潜力角度）。
- dev_complexity 评估：能否在 2-4 周内实现核心功能？需要什么技术栈？
- monetization_hint 推测：基于应用类型，最可能的变现方式是什么？
- 只输出合法 JSON，不要 Markdown、解释、注释或尾随逗号。"""

_CAPTURE_USER = """以下是 Apple 应用榜单原始数据。它是唯一事实来源，字段通常包括 `name`、`artist`、`apple_id`、`rank`；缺失字段不得猜测:

{apps_json}

请从中精选最多 {max_benchmarks} 个最符合标准的商业机会。按"商业价值"排序：优先市场需求确定、开发难度低、变现潜力高的项目。每个元素必须包含以下字段:
[
  {{
    "name": "应用名（原样保留）",
    "artist": "开发者（原样保留）",
    "apple_id": "保留原数据中的 apple_id，没有则空字符串",
    "jtbd": "基于名称和类别线索的最小 JTBD；使用'当我...时，我想要...，以便...'格式，不得添加输入没有支持的复杂功能",
    "search_keywords": ["用于检索同一用户任务的中文关键词", "2-4 个，至少一个场景词和一个类型词"],
    "pick_reason": "商业机会评估：为何适合独立开发者抢占鸿蒙市场？从以下角度说明：1)Apple榜单排名证明的市场需求 2)独立开发的可行性 3)可能的变现路径 4)鸿蒙供给缺口预判",
    "dev_complexity": "low 或 medium 或 high - 评估独立开发者实现难度。low=2周内可实现核心功能，medium=2-4周，high=超过4周或需要特殊资源",
    "monetization_hint": "付费下载 或 应用内购买 或 订阅制 或 广告变现 或 免费+增值服务 或 引流工具 - 基于应用类型推测最可能的变现方式"
  }}
]

如果没有足够符合条件的项目，返回空数组。只输出 JSON 数组本身。"""

_PROBE_FILTER_SYSTEM = """你是"供给缺口探测器"。判断华为应用市场的候选应用能否满足标杆的用户任务。你的目标是找出"鸿蒙上没有、或有但质量差"的商业机会点。

判定规则：
- 必须同时满足：用户目标基本相同、使用场景基本相同、候选确实是应用而非内容/平台入口。
- **关键判断：候选应用能否"足够好地"完成标杆的 JTBD？**
  即使找到了同类应用，如果存在以下情况，仍然算"供给不足"（保留在结果中）：
  * 功能残缺：只实现了部分核心功能
  * 体验差：评分低于 3.5、差评集中抱怨可用性
  * 勉强沾边：名称相关但实际解决的是另一个任务
  * 更新停滞：超过 1 年未更新，可能已不兼容新系统
- 仅关键词相同、同属大类、或能作为替代品但解决的是另一任务，不算同类。
- category、name、intro 只代表输入线索；缺少足够证据时剔除，不能用常识补全功能。
- 永久剔除游戏、金融证券、社交、电商、视频/资讯、泛平台工具，以及明显的广告/下载/壁纸/清理类噪声。
- 按 JTBD 契合度排序，保留数量不超过用户消息中的上限；没有确定匹配就返回空数组。
- **商业化视角：如果找到 3 个以上真正的同类竞品且都质量不错，说明市场已饱和，直接返回空数组（不是机会）。**
- 合法的空数组 `[]` 表示两种情况：1) 没有确认的同类竞品（市场空白）2) 竞品过多且质量都好（市场饱和）。
- package 必须从候选列表中逐字复制；不确定时不得输出，更不得把不确定转写成「市场不存在」的结论。

只输出合法 JSON 数组（包名字符串），不要解释、Markdown 或其他字段。"""

_PROBE_FILTER_USER = """请判断以下候选应用中，哪些与标杆产品属于“真正同类竞品”。只依据下方输入，不要补充外部知识：

标杆名称: {benchmark_name}
标杆 JTBD（用户底层任务）: {benchmark_jtbd}

华为应用市场候选列表（每项含 name/category/package/intro，字段可能为空）:
{candidates_json}

返回 JSON 数组，只包含候选列表中原样出现的 `package` 字符串，按契合度排序，最多 {max_competitors} 个。不得改写包名、不得输出列表外包名。若确无同类，返回合法空数组 `[]`；格式异常不得伪装成 `[]`。不确定时宁可不输出，也不要把不确定当作「市场不存在」的结论。"""

_PROBE_PAIN_SYSTEM = """你是用户反馈编码器。把输入的真实低分评论归并为可用于产品决策的痛点，不负责替用户解释动机，也不负责提出解决方案。

规则：
- 只引用和概括输入评论；不得添加评论中没有出现的广告、崩溃、隐私、性能、功能或商业结论。
- 将同义抱怨合并，最多 5 个，按评论中出现的频率和严重性排序；评论不足时可以少于 3 个。
- frequency 只能从“高频/中频/低频”中选择；这是在当前输入样本内的相对频率，不是全市场统计。
- quote 必须是输入中的连续原文短片段，可适度截短但不要改写、拼接或伪造。
- 使用简体中文。评论原文若为繁体或其他语言，quote 保留原文；issue/note 用简体中文。
- 输入为空或没有有效文本时，输出 {{"pain_points": [], "note": "暂无有效低分评论"}}。

只输出合法 JSON 对象，不要解释或 Markdown。"""

_PROBE_PAIN_USER = """以下是从华为应用市场抓取的真实低分用户评论（rating <= 3）。只把它们视为样本，不要推断全部用户:
{reviews_json}

请将上述评论中的可重复痛点提炼为 JSON。若同一问题只出现一次，也可保留但 frequency 必须为“低频”:
{{
  "pain_points": [
    {{
      "issue": "痛点描述（一句话）",
      "frequency": "高频/中频/低频",
      "quote": "输入评论中的连续原文片段，不得编造"
    }}
  ],
  "note": "仅概括本批样本已显示的问题；没有证据的维度不要提及"
}}

只输出 JSON 对象本身；pain_points 没有内容时必须为空数组。"""

_USER_FILTER_SYSTEM = """你是独立开发者的应用机会筛选助理。你的任务是通读用户的应用数据库，按用户写明的标准挑出真正值得做的方向，并说明理由。

事实纪律（严格执行）：
- 只能使用输入数据库中的事实：应用名、JTBD、入选理由、榜单来源、竞品数量与评分、差评痛点原文。
- 禁止虚构下载量、收入、评分趋势、用户规模、功能细节或鸿蒙平台能力；未知信息不得当作不存在。
- 「生态空白」只代表最近一次检索未发现同类竞品，属于待验证假设，不得表述为全市场结论。
- attack_vector 必须对应输入中真实出现的差评痛点或检索空白；没有证据时写「待验证」。
- 不得因为应用没被 probe 过就判定它没有机会，只能标注「尚未验证供给」。

输出纪律：
- 只输出合法 JSON 数组，不要 Markdown、解释、注释或尾随逗号。
- 数组元素必须包含 name、apple_id、jtbd、why_pick、attack_vector、native_features、competitors_summary 七个字段。
- 按你判断的潜力从高到低排序，数量不超过用户消息中的上限。
- 没有符合条件的应用时返回空数组。"""

_USER_FILTER = """你是我的个人应用机会筛选助理。

我的目标：找到适合独立开发者在鸿蒙生态做的小而美产品方向。

筛选标准：
1. 单一场景，边界清晰，不是平台型产品
2. 有真实需求验证（Apple 榜单出现 或 华为竞品有差评）
3. 适合独立开发者（2-4 周 MVP，无需复杂后端或内容运营）
4. 能用鸿蒙原生能力加分（实况窗/卡片/小艺/端侧 AI）

优先类型：
- 工具类（生产力、效率、创作辅助）
- 生活方式（健康、习惯养成、个人管理）
- 摄影/音频（滤镜、后期、播客工具）

排除：
- 游戏、社交、电商、金融、新闻资讯
- 大厂全家桶、需要 UGC 运营的产品
- 需要复杂后端服务的产品

输入数据：
以下是我的应用数据库（Apple 应用信息 + 华为竞品调研）：

{database_json}

输出要求：
按你认为最有潜力的顺序（综合考虑：需求验证、开发可行性、竞争态势、原生能力契合度），返回最多 {max_results} 个机会。

JSON 格式：
[
  {{
    "name": "应用名",
    "apple_id": "Apple ID",
    "jtbd": "用户任务",
    "why_pick": "为什么适合我（2-3句话，说明需求验证 + 开发可行性 + 原生能力）",
    "attack_vector": "主要切入点（生态空白 或 竞品具体痛点）",
    "native_features": ["推荐的鸿蒙能力1", "推荐的鸿蒙能力2"],
    "competitors_summary": "华为竞品概况（数量/评分/主要痛点，1句话）"
  }}
]

只输出 JSON 数组，不要解释。"""

# 有序模板清单：name → spec（label 供 UI 展示，placeholders 供占位符校验）
PROMPT_TEMPLATES: dict[str, dict] = {
    "capture_system": _spec("榜单精选 · System", "M1 capture", "system", _CAPTURE_SYSTEM),
    "capture_user": _spec("榜单精选 · User", "M1 capture", "user", _CAPTURE_USER,
                          ["apps_json", "max_benchmarks"]),
    "probe_filter_system": _spec("竞品语义过滤 · System", "M2 probe", "system",
                                 _PROBE_FILTER_SYSTEM),
    "probe_filter_user": _spec("竞品语义过滤 · User", "M2 probe", "user", _PROBE_FILTER_USER,
                               ["benchmark_name", "benchmark_jtbd", "candidates_json",
                                "max_competitors"]),
    "probe_pain_system": _spec("差评痛点提炼 · System", "M2 probe", "system",
                               _PROBE_PAIN_SYSTEM),
    "probe_pain_user": _spec("差评痛点提炼 · User", "M2 probe", "user", _PROBE_PAIN_USER,
                             ["reviews_json"]),
    "user_filter_system": _spec("机会筛选 · System", "AI filter", "system",
                                _USER_FILTER_SYSTEM),
    "user_filter": _spec("机会筛选 · User", "AI filter", "user", _USER_FILTER,
                         ["database_json", "max_results"]),
}

# 真实占位符：单花括号 {name}；`{{`/`}}` 是 .format() 的字面量转义，不算占位符
_PLACEHOLDER_RE = re.compile(r"(?<!\{)\{([a-zA-Z_][a-zA-Z0-9_]*)\}(?!\})")


def prompt_path(name: str):
    """返回某模板的用户自定义文件路径（不保证存在）。"""
    return PROMPTS_DIR / f"{name}.txt"


def get_prompt(name: str) -> str:
    """按「用户自定义文件 > 内建默认兜底」策略解析模板文本。"""
    spec = PROMPT_TEMPLATES.get(name)
    if spec is None:
        raise KeyError(f"未知提示词模板: {name}")
    path = prompt_path(name)
    if path.exists():
        try:
            text = path.read_text(encoding="utf-8")
            if text.strip():
                return text
        except OSError:
            pass
    return spec["default"]


def is_customized(name: str) -> bool:
    """用户是否已自定义该模板（文件存在且非空且与默认不同）。"""
    path = prompt_path(name)
    if not path.exists():
        return False
    try:
        return path.read_text(encoding="utf-8") != PROMPT_TEMPLATES[name]["default"]
    except OSError:
        return False


def find_placeholders(text: str) -> set[str]:
    """提取模板中真实占位符名（忽略 {{ }} 转义与 JSON 字面量）。"""
    return set(_PLACEHOLDER_RE.findall(text or ""))


def validate_prompt(name: str, content: str) -> list[str]:
    """校验模板：非空 + 必要占位符齐全 + 可被 str.format 安全调用。返回错误列表。"""
    spec = PROMPT_TEMPLATES.get(name)
    if spec is None:
        return [f"未知提示词模板: {name}"]
    errors: list[str] = []
    if not (content or "").strip():
        return ["模板内容不能为空。"]

    found = find_placeholders(content)
    missing = [p for p in spec["placeholders"] if p not in found]
    if missing:
        errors.append("缺少必要占位符: " + ", ".join("{%s}" % p for p in missing))

    # 试格式化：用占位符名作值，暴露未转义花括号等语法问题
    try:
        content.format(**{p: "" for p in found})
    except (KeyError, IndexError, ValueError) as e:
        errors.append(f"占位符语法有误（花括号未配对或非法占位符）: {e}")
    return errors


def save_prompt(name: str, content: str) -> str:
    """原子写入用户自定义模板到 prompts/<name>.txt，返回文件路径。"""
    if name not in PROMPT_TEMPLATES:
        raise KeyError(f"未知提示词模板: {name}")
    PROMPTS_DIR.mkdir(parents=True, exist_ok=True)
    target = prompt_path(name)
    fd, tmp = tempfile.mkstemp(dir=str(PROMPTS_DIR), prefix=f".{name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, target)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    return str(target)


def reset_prompt(name: str) -> None:
    """恢复某模板为内建默认（删除用户自定义文件）。"""
    path = prompt_path(name)
    if path.exists():
        path.unlink()


def reset_all_prompts() -> None:
    """恢复全部模板为内建默认。"""
    for name in PROMPT_TEMPLATES:
        reset_prompt(name)


def export_defaults(overwrite: bool = False) -> list[str]:
    """将内建默认模板导出为 prompts/*.txt（便于用户直接编辑）。

    overwrite=False 时不覆盖已存在的用户文件。返回实际写入的文件路径列表。
    """
    written = []
    for name, spec in PROMPT_TEMPLATES.items():
        path = prompt_path(name)
        if path.exists() and not overwrite:
            continue
        written.append(save_prompt(name, spec["default"]))
    return written


# ============================================================
# 模块级常量：import 时解析一次，供 pipeline 直接使用
# ============================================================
CAPTURE_SYSTEM = get_prompt("capture_system")
CAPTURE_USER = get_prompt("capture_user")
PROBE_FILTER_SYSTEM = get_prompt("probe_filter_system")
PROBE_FILTER_USER = get_prompt("probe_filter_user")
PROBE_PAIN_SYSTEM = get_prompt("probe_pain_system")
PROBE_PAIN_USER = get_prompt("probe_pain_user")
USER_FILTER_SYSTEM = get_prompt("user_filter_system")
USER_FILTER_USER = get_prompt("user_filter")


def snapshot() -> dict[str, str]:
    """返回当前生效的全部模板快照（用于运行批次归档）。"""
    return {name: get_prompt(name) for name in PROMPT_TEMPLATES}
