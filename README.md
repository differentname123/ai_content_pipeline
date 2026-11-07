# AI Content Pipeline (欧易 OKX & 币安 Binance 帖子采集、清洗、AI生成与多平台发布系统)

本项目是从原仓库独立剥离出的**纯内容流水线模块（不包含任何实盘交易代码或交易密钥）**，包含完整的多平台帖子拉取、图片感知哈希去重、AI 初筛与 MLU 提取、分析/爆款文章生成、Playwright 自动发布、评论区互动以及历史帖子清理工具，支持 Windows / Linux / macOS 跨平台独立运行。

---

## 目录结构

```text
ai_content_pipeline/
├── config/
│   ├── config.json                 # 核心配置文件（MongoDB、AI网关、代理、浏览器会话目录）
│   └── gemini_web.json             # 备用 Gemini 配置文件
├── prompt/
│   └── 内容生成方案_*.txt            # 分析类/娱乐爆款类全套 Prompt 模板
├── common/
│   ├── common_utils.py             # 统一路径解析、代理管理、日志、图片感知哈希(dHash)、媒体下载
│   └── mongo_db/
│       ├── mongo_base.py           # MongoDB 连接池单例
│       └── mongo_manager.py        # UniversalPostManager & GeneratedArticleManager
├── app/
│   └── ai_api/
│       ├── model_api.py            # OpenAI 兼容网关调用、重试降级与并发配额管理
│       ├── gemini_api.py           # (备用) Google GenAI SDK 封装
│       ├── gemini_playwright.py    # (备用) AI Studio Playwright 封装
│       └── gemini_web.py           # (备用) Gemini Web 端封装
├── biance/
│   ├── biance_squre_api.py         # 币安广场推荐流/热榜/评论拉取、点赞收藏、API删帖、CCXT最新价
│   ├── biance_playwright.py        # 币安广场 Playwright 自动发帖、评论、鉴权提取
│   ├── okx_squre_api.py            # 欧易 Orbit 推荐流/热榜/用户主页时间线拉取与清洗
│   └── okx_playwright.py           # 欧易 Orbit Playwright 自动发帖、评论、页面批量删帖
├── parser/
│   ├── article_format_logical.py   # 主控调度流水线
│   └── account_publish_state.json  # 多账号发布冷却状态记录
└── reset_all_fresh_start.py        # 一键清空远端帖子、MongoDB 帖子集合与本地缓存运维脚本
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
  - 自动兼容 Windows 原有路径（若当前机器存在对应盘符如 `W:\` 则直接使用）。
  - 若在 Linux / macOS 或无 `W:` 盘的环境下运行，会自动回退至项目根目录下的 `./browser_sessions/<platform>_<account>`。
- **浏览器内核 (`browser_channel`)**：
  - 默认 `"chrome"`；若服务器未安装系统级 Chrome，会自动回退至 Playwright 内置的 `"chromium"`。

### 3. 启动主流水线

在项目根目录下直接运行：

```bash
python parser/article_format_logical.py
```

### 4. 全量重置（可选运维工具）

```bash
python reset_all_fresh_start.py
```
