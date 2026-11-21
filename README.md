# AI Content Pipeline (Binance / OKX / X 多平台自动化内容引擎与增长流水线)

本项目是面向 **Binance 广场、OKX 星球 (Orbit) 与 X (Twitter)** 的自动化内容采集、多模态 AI 筛选创作、总编擂台选稿发布与高质量神评互动系统，支持 Windows / Linux / macOS 跨平台无人值守运行。

> [!IMPORTANT]
> **开发与运维必读**：修改任何代码、配置或提示词前，必须先阅读并严格遵守 [`PROJECT_PRINCIPLES.md`](PROJECT_PRINCIPLES.md) 与 [`AGENTS.md`](AGENTS.md) 中的核心安全与架构红线（免登优先、`x_myself` 主号采集物理隔离、`model=None` + `preset_model_group` 统一模型路由、`platforms/` 过滤注解完整等）。

---

## 核心架构与活跃模块

系统收束为四层正交结构与 **4 个常驻生产主进程**：

```text
ai_content_pipeline/
├── PROJECT_PRINCIPLES.md             # 设计理念与维护红线
├── AGENTS.md                         # AI 协作者核心红线守则
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
├── platforms/                        # 平台接口、共享 Playwright 底座与 X 中文爆款自动发帖
│   ├── playwright_base.py            # `persistent_browser_session`、真实 Chrome 无头伪装、浮层看门狗、WAF识别、取证
│   ├── binance_api.py / binance_playwright.py
│   ├── okx_api.py / okx_playwright.py
│   ├── x_api.py                      # X 历史抓取、`fxtwitter` + `syndication` 双通道免登查询、小号防主号隔离
│   ├── x_playwright.py               # X 发帖/回复/关注（含媒体上传就绪守卫与零重发安全重试）
│   └── x_auto_publisher.py           # 【生产守护进程 2】X 中文爆款精选改写与自动发帖
│
├── parser/                           # 【生产守护进程 1】Binance / OKX 内容采编发布流水线
│   ├── config.py                     # 统一调参中心（提示词版本、冷却时长、阈值、互动开关、字数规则）
│   ├── publish_state.py              # 线程安全账号发帖状态与间隔冷却中心（不启用失败退避阻塞）
│   ├── platform_adapters.py          # 双平台发帖格式、热币缓存、竞品词检查、免登阅读/互动指标同步
│   ├── post_ingestion.py             # 双平台热榜/推荐流采集与原帖校验归一化
│   ├── mlu_pipeline.py               # 分析帖纯文本初筛与多模态论据（MLU）精提
│   ├── article_generator.py          # 高流量一阶段粗筛、单币分析生成、高流量二阶段洗稿（记录真实 model_used）
│   ├── publisher_arena.py            # 7 天 dHash 图片去重、发布前账号零成本预检、Top-50 总编擂台选稿发布
│   ├── maintenance_tasks.py          # 评论区互评/点赞、按时间/行情删帖与人工抽样复盘工具
│   └── article_format_logical.py     # 任务总开关（TASK_SWITCHES 首屏配置）与主控入口（`--once` / `--tasks` / `--dry-run`）
│
├── x_growth/                         # 【生产守护进程 3 & 4】X 英/日蓝 V 神评增长与每小时自动互关流水线
│   ├── growth_config.py              # 采集/生成/多账号发评/自动关注统一配置中心
│   ├── x_browser_collector.py        # 真实 Chrome GraphQL 被动嗅探采集（严禁触碰 `x_myself`）
│   ├── comment_generator.py          # `low` 批量生成与分类 + `medium` 独立盲测打分（20 条/波清零）
│   ├── comment_pipeline.py           # 【生产守护进程 3】采集 + 生成一键串联守护进程
│   ├── comment_publisher.py          # 【生产守护进程 4】主号神评消费、防爆楼（<=15 评 / >=75 分）与免登指标回流
│   ├── follow_selector.py            # 高互关概率账号筛选评分（S/A/B）、暖线索识别与回关核验台账
│   └── auto_follower.py              # 每小时自动补采+双号关注守护进程（含 `--dry-run` / `--status` / `--login`）
│
├── prompt/                           # 当前活跃的 11 个生产/人工复盘提示词模板
└── tests/                            # 确定性离线单元测试套件（零真实网络/数据库/浏览器副作用）
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. 核心运行入口（均支持 `--help` 查看参数）

- **Binance / OKX 采集-生产-总编发布主流水线**：
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
- **X 高互关账号筛选与每小时自动关注**：
  ```bash
  python x_growth/auto_follower.py                   # 默认每小时巡检一次，为 x_myself 与 x_dahao 执行互关
  python x_growth/auto_follower.py --dry-run         # 零浏览器预览当前 S/A/B 级候选名单
  python x_growth/auto_follower.py --status          # 只读查看候选池、24h 配额与各档真实回关率
  python x_growth/auto_follower.py --once --check-followbacks
  python x_growth/auto_follower.py --login x_dahao   # 首次为 x_dahao 扫码登录固化浏览器目录
  ```
- **人工扫码/登录浏览器会话接管**：直接运行 `platforms/binance_playwright.py`、`platforms/okx_playwright.py` 或 `platforms/x_playwright.py`。

### 3. 运行单元测试

```bash
python -m unittest discover -s tests -v
```
