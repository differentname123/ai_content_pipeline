# AI Content Pipeline (Binance / OKX / X 多平台自动化内容引擎与增长流水线)

本项目是面向 **Binance 广场、OKX 星球 (Orbit) 与 X (Twitter)** 的自动化内容采集、多模态 AI 筛选创作、总编擂台选稿发布与高质量神评互动系统，支持 Windows / Linux / macOS 跨平台无人值守运行。

> [!IMPORTANT]
> **开发与运维必读**：修改任何代码、配置或提示词前，必须先阅读并严格遵守 [`PROJECT_PRINCIPLES.md`](PROJECT_PRINCIPLES.md) 与 [`AGENTS.md`](AGENTS.md) 中的核心安全与架构红线（免登优先、`x_myself` 主号采集物理隔离、`model=None` + `preset_model_group` 统一模型路由、`platforms/` 过滤注解完整等）。涉及币安广场运营调整请查阅 [`BINANCE_SQUARE_OPERATIONS.md`](BINANCE_SQUARE_OPERATIONS.md)；**涉及 X (Twitter) 内容运营、算法机制、风控红线与自动化执行规范，请务必先阅读实证守则 [`X_PLATFORM_OPERATIONS.md`](X_PLATFORM_OPERATIONS.md)**。

---

## 核心架构与活跃模块

系统收束为五层正交结构，覆盖 **币圈垂直发帖、跨五平台全领域热点感知与原创生产、X 自动增长互动** 共 **7 个核心运行入口**：

```text
ai_content_pipeline/
├── PROJECT_PRINCIPLES.md             # 设计理念与维护红线
├── AGENTS.md                         # AI 协作者核心红线守则
├── BINANCE_SQUARE_OPERATIONS.md      # 币安广场实证流量诊断与运营守则
├── X_PLATFORM_OPERATIONS.md          # X 平台实证机制、正负规则与自动化执行守则
├── requirements.txt                  # 运行时精准依赖清单
├── config/config.json                # MongoDB、模型网关权重、代理、会话目录配置
│
├── common/                           # 共享工程与领域原子层（单一事实来源）
│   ├── common_utils.py               # 配置缓存、路径解析、跨平台常量、dHash 图片去重、自营账号识别
│   ├── daemon.py                     # 统一可中断守护循环（jitter 抖动、动态冷却休眠、异常显式感知）
│   ├── llm_gateway.py                # 提示词模板渲染、JSON 解析/自愈重试、`preset_model_group` 网关
│   ├── post_utils.py                 # `x_myself` 隔离守卫、外链可读性过滤、视频判断、统一正文抽取
│   ├── x_text.py                     # X 平台加权字长（280 单位）与标准裸字符长度校验
│   └── mongo_db/
│       ├── mongo_base.py             # MongoDB 单例连接池
│       └── mongo_manager.py          # UniversalPostManager / GeneratedArticleManager / HarvestedPostManager
│
├── app/ai_api/model_api.py           # OpenAI 兼容网关统一调度、多模型负载均衡与文件锁审计
│
├── platforms/                        # 平台接口、共享浏览器底座、五大平台采集与 X 被动执行守护
│   ├── playwright_base.py            # `persistent_browser_session`、真实 Chrome 会话与通用页面原语
│   ├── x_browser.py                  # X 专用 Patchright + 本机正版有头 Chrome 引擎、全物理输入与 48h 熔断器
│   ├── binance_api.py / binance_playwright.py
│   ├── okx_api.py / okx_playwright.py
│   ├── x_api.py                      # X 历史抓取、`fxtwitter` + `syndication` 双通道免登查询、小号防主号隔离
│   ├── x_playwright.py               # X 原子页动作（发帖/回复/关注/置顶/只读通知嗅探 + 响应级风控拦截）
│   ├── x_ops_daemon.py               # X 纯被动单 Chrome 任务执行守护（`enqueue_x_action` / `wait_x_action` / 配额顺延）
│   ├── external_link_extractor.py    # 站外长文正文与头图通用抓取提取器
│   ├── harvesters/                   # 五大平台采集器（`binance` / `okx` / `x` / `reddit` / `hackernews` + `base_harvester.py`）
│   ├── harvester_runner.py           # 【五大平台采集总控】广谱每小时定时全量采集与按关键词定向深挖统一入口
│   └── x_auto_publisher.py           # 【生产守护进程 2】X 中文爆款精选改写与自动发帖
│
├── content_pipeline/                 # 【跨五平台热点感知与原创图文总控】通用全领域多模态生产流水线
│   ├── pipeline_config.py / pipeline_db.py / pipeline_logger.py / llm_gateway.py
│   ├── step2_comprehension.py        # 单帖多模态无损理解档案（`pipeline_post_digests`）
│   ├── topic_radar.py                # 跨五平台热点方向发现雷达与多批归并
│   ├── direction_expander.py         # 按选定方向跨五大平台「指哪打哪」关键词深挖
│   ├── material_curator.py           # 方向素材精选去噪与空心方向熔断
│   ├── content_creator.py / visual_studio.py  # 解耦原创图文创作与真实原图无损挑选
│   ├── blind_reviewer.py             # 100% 上下文物理隔离普通网友盲测终审（`pipeline_creations`）
│   ├── comment_creator.py            # 【独立旁路工具】单帖多语种神评候选生成
│   └── pipeline_runner.py            # 全自动双环/人工指定方向/纯素材直通一键总控入口
│
├── parser/                           # 【生产守护进程 1】Binance / OKX 内容采编发布流水线
│   ├── config.py                     # 统一调参中心（提示词版本、账号级全局冷却、阈值、互动开关、字数规则）
│   ├── publish_state.py              # 线程安全账号级全局冷却、并发占锁与状态中心（不启用失败退避阻塞）
│   ├── platform_adapters.py          # 双平台发帖格式、竞品词与竞品UI分流检查、精准标签、免登阅读/风控指标同步
│   ├── post_ingestion.py             # 双平台热榜/推荐流采集与原帖校验归一化
│   ├── mlu_pipeline.py               # 分析帖纯文本初筛与多模态论据（MLU）精提（含 `exchange_ui` 客观标注）
│   ├── article_generator.py          # 高流量一阶段粗筛、单币分析生成、高流量二阶段洗稿（记录真实 model_used）
│   ├── publisher_arena.py            # 7 天 dHash 图片去重、竞品UI跨平台分流、发布前账号并发锁与预检、Top-50 总编擂台
│   ├── maintenance_tasks.py          # 评论区互评/点赞、按时间/行情删帖与人工抽样复盘工具
│   └── article_format_logical.py     # 任务总开关（TASK_SWITCHES 首屏配置）与主控入口（`--once` / `--tasks` / `--dry-run`）
│
├── x_growth/                         # 【生产守护进程 3 & 4】X 英/日蓝 V 神评增长与每小时自动互关流水线
│   ├── growth_config.py              # 采集/生成/多账号发评/自动关注统一配置中心
│   ├── x_browser_collector.py        # 真实 Chrome GraphQL 被动嗅探采集（严禁触碰 `x_myself`）
│   ├── comment_generator.py          # `low` 批量生成与分类 + `medium` 独立盲测打分（20 条/波清零）
│   ├── comment_pipeline.py           # 【生产守护进程 3】采集 + 生成一键串联守护进程
│   ├── comment_publisher.py          # 【生产守护进程 4】主号神评消费、防爆楼（<=15 评 / >=75 分）与免登指标回流
│   └── auto_follower.py              # 极简高内聚自动互关脚本（`x_user_profiles` 首页+评论+蓝V粉丝圈评分 + `x_follow_records` 分账号关注）
│
├── prompt/                           # 当前活跃的 20 个生产/多阶段提示词模板（含 `pipeline_v2/` 8 个与 `x_growth/` 2 个）
└── tests/                            # 确定性离线单元测试套件（零真实网络/数据库/浏览器副作用）
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. 核心运行入口

- **五大平台（X / Reddit / HackerNews / Binance / OKX）广谱全量采集总控**：
  ```bash
  python platforms/harvester_runner.py               # 默认按文件顶部 GLOBAL_SCHEDULE_CONFIG["continuous_loop"] 每小时常驻采集入库（harvested_social_posts）
  ```
- **跨五平台全领域热门题材感知与原创图文生产总控**：
  ```bash
  python content_pipeline/pipeline_runner.py         # 按 content_pipeline/pipeline_config.py 开关执行：单帖理解 -> 热点雷达 -> 跨五平台深挖 -> 素材精选 -> 图文创作 -> 盲测终审
  python content_pipeline/comment_creator.py         # 独立旁路示例：单帖多语种神评生成
  ```
- **Binance / OKX 采集-生产-总编发布主流水线**（支持 `--help` 查看参数）：
  ```bash
  python parser/article_format_logical.py            # 启动 7 个启用后台线程常驻运行
  python parser/article_format_logical.py --dry-run  # 只读检查任务清单与账号可用/冷却状态
  python parser/article_format_logical.py --once --tasks single_analysis_pub
  ```
- **X 中文爆款自动选帖与发布**：
  ```bash
  python platforms/x_auto_publisher.py               # 每 3 小时精选 6h 内高分爆款并自动改写发帖
  python platforms/x_auto_publisher.py --help
  ```
- **X 英/日蓝 V 被动采集与神评生成串联**：
  ```bash
  python x_growth/comment_pipeline.py                # 默认 ~40m + jitter 常驻采集并清零生成
  python x_growth/comment_pipeline.py --once
  ```
- **X 神评消费发布与免登指标同步**：
  ```bash
  python x_growth/comment_publisher.py               # 常驻消费（24 条/天，replies<=15，score>=75）
  python x_growth/comment_publisher.py --once --dry-run
  python x_growth/comment_publisher.py --sync-metrics
  ```
- **X 极简高内聚每小时自动互关**（直接在 [x_growth/auto_follower.py](x_growth/auto_follower.py) 顶部修改 `RUN_MODE` 与 `TARGET_ACCOUNTS`）：
  ```bash
  python x_growth/auto_follower.py                   # 按代码顶部 RUN_MODE 运行（默认 "daemon" 每小时采集推荐流/评论区/高分蓝V的 /verified_followers 并关注 20/轮、400/天；间隔 3~5s + 180s 持锁止损，满 48h 未回关自动取关）
  # 可选在代码顶部切换 RUN_MODE = "harvest_once" / "follow_once" / "unfollow_once" / "preview" / "status" / "login"
  ```
- **人工扫码/登录浏览器会话接管**：直接运行 `platforms/binance_playwright.py`、`platforms/okx_playwright.py` 或 `platforms/x_playwright.py`。

### 3. 运行单元测试

```bash
python -m unittest discover -s tests -v
```
