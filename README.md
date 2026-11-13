# AI Content Pipeline (欧易 OKX / 币安 Binance / X 帖子采集、清洗、AI生成与多平台发布系统)

本项目是**多平台内容采集、多模态 AI 理解与原创图文自动化生产/发布系统**，支持 Windows / Linux / macOS 跨平台独立运行。

> [!IMPORTANT]
> **开发与运维必读宪章**：任何后续参与维护、修改拉取逻辑、调优提示词或扩展渠道的人员与 AI 代理，动手前**必须先阅读 [《项目核心设计理念与维护红线 (`PROJECT_PRINCIPLES.md`)》](PROJECT_PRINCIPLES.md)**（涵盖「简单稳定 80/20 与全自动自愈」、「免登优先 + 小号最小特权 + 主号隔离」、「拉取层严禁越界限制且保留过滤必写说明」及「流水线正交解耦」五大铁律）。

---

## 目录结构

```text
ai_content_pipeline/
├── PROJECT_PRINCIPLES.md           # 【核心必读】系统设计理念与维护红线宪章
├── AGENTS.md                       # AI 协作代理自动加载的维护红线守则
├── config/
│   └── config.json                 # 核心配置文件（MongoDB、AI网关、代理、浏览器会话目录）
├── prompt/
│   ├── pipeline_v2/                # 模块化流水线专用抽象原则提示词模板（.md）
│   └── 内容生成方案_*.txt            # 旧版历史 Prompt 模板
├── content_pipeline/               # 新一代全领域多模态内容理解与原创图文生产流水线（与旧版物理隔离）
│   ├── pipeline_config.py          # 全局配置中心（Profile 切换、阶段开关、循环调度、模型组配置）
│   ├── pipeline_runner.py          # 全链路模块化编排总控入口（IDE 直接运行）
│   ├── step2_comprehension.py      # 模块 2：单帖「正文+外链全文+原图+热评」多模态理解
│   ├── topic_radar.py              # 模块 3：广谱热点方向与搜索词发现雷达
│   ├── direction_expander.py       # 模块 4：按方向跨五平台「指哪打哪」关键词深挖与即时理解
│   ├── material_curator.py         # 模块 5：方向素材 AI 精选去噪
│   ├── content_creator.py          # 模块 6：解耦原创图文创作
│   ├── blind_reviewer.py           # 模块 7：100% 上下文物理隔离普通网友盲测终审
│   ├── comment_creator.py          # 模块 8：独立全语种帖子互动神评生成
│   ├── visual_studio.py            # 真实原图无损挑选与确定性处理模块
│   ├── llm_gateway.py              # 提示词版本指纹渲染与统一模型组网关封装
│   ├── pipeline_db.py              # 独立 MongoDB 集合读写层（pipeline_*）
│   └── pipeline_logger.py          # 高信噪比主日志与全链路可回溯 Trace 归档
├── common/
│   ├── common_utils.py             # 统一配置缓存、路径解析、跨平台常量/Schema契约、图片dHash、媒体下载
│   └── mongo_db/
│       ├── mongo_base.py           # MongoDB 连接池单例
│       └── mongo_manager.py        # UniversalPostManager & GeneratedArticleManager
├── app/
│   └── ai_api/
│       └── model_api.py            # OpenAI 兼容网关调用、preset_model_group 权重模型组与自动降级
├── platforms/
│   ├── harvester_runner.py         # Step 1：多平台纯粹无损采集常驻调度器（存入 harvested_social_posts）
│   ├── harvesters/                 # X / Reddit / HackerNews / Binance / OKX 标准化采集器实现
│   ├── playwright_base.py          # Playwright 跨平台共享自动化底座（可重入目录锁、浮层免疫、编辑器控制、故障取证）
│   ├── binance_api.py              # 币安广场推荐流/热榜/评论拉取、点赞收藏、API删帖、CCXT行情与涨跌榜
│   ├── binance_playwright.py       # 币安广场 Playwright 自动发帖、评论、鉴权提取
│   ├── okx_api.py                  # 欧易 Orbit 推荐流/热榜/用户主页时间线拉取与清洗
│   ├── okx_playwright.py           # 欧易 Orbit Playwright 自动发帖、评论、WebCrypto签名批量删帖
│   ├── x_api.py                    # X (Twitter) GraphQL 监控采集、精选筛选与标准Schema转换
│   └── x_playwright.py             # X (Twitter) Playwright 自动发帖（图文/纯文本）、评论回复与点赞互动
├── parser/
│   └── article_format_logical.py   # 旧版主控调度流水线（支持独立启停采集、生成、发布、互动、清理任务）
└── logs/
    ├── harvester.log               # Step 1 多平台无损采集精炼运维日志
    ├── content_pipeline.log        # 新一代内容理解与创作流水线主日志
    ├── app_YYYY-MM-DD.log          # 旧版统一运行日志
    ├── forensics/                  # Playwright 故障现场截图/HTML/诊断JSON
    └── runtime/                    # 运行时状态与全量审计档案（latest_harvester_report.json、pipeline_traces/ 等）
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. 跨平台配置说明 (`config/config.json`)

- **代理配置 (`proxy_url`)**：
  - 本地开发环境默认 `"http://127.0.0.1:7890"`。
  - 若部署在海外 Linux 服务器无需代理，只需将 `config/config.json` 中的 `"proxy_url"` 设为空字符串 `""` 或 `null`（或设置环境变量 `AI_PIPELINE_PROXY=none`），全链路（Requests / Httpx / CCXT）将自动切换为直连模式。
- **浏览器会话目录 (`*_browser_session_dir`)**：
  - 自动兼容 Windows 原有路径（若当前机器存在对应盘符如 `E:\` / `W:\` 则直接使用）。
  - 若在 Linux / macOS 或无对应盘符的环境下运行，会自动回退至项目根目录下的 `./browser_sessions/<platform>_<account>`。
- **浏览器内核 (`browser_channel`)**：
  - 默认 `"chrome"`；若服务器未安装系统级 Chrome，会自动回退至 Playwright 内置的 `"chromium"`。

### 3. 运行入口

- **新一代无损采集器（Step 1）**：直接在 IDE 运行 `platforms/harvester_runner.py`（配置见顶部 `HARVESTER_CONFIG`）。
- **新一代多模态理解与原创生产流水线（Step 2 ~ Step 4）**：直接在 IDE 运行 `content_pipeline/pipeline_runner.py`（或单独运行各阶段模块调试，配置见 `content_pipeline/pipeline_config.py`）。
- **旧版全流程调度脚本**：直接运行 `parser/article_format_logical.py`（可在 `TASK_SWITCHES` 与 `main()` 中独立控制各子任务启停）。
