# `x_growth` 实证决策与埋点观测台账（基于 `N=68` 线上复盘，2026-11-19）

原则：**仅让多轮统计学显著且实测有效的机制控制线上选帖与发评；未经大样本验证或存在混杂的猜想一律不强行加入过滤/排序，通过 `auto_comment.publish_telemetry` 持续埋点记录，待样本充足后再做裁决。**

---

## 一、已验证有效并锁定落地的机制

| 模块 / 规则 | 证据与原因 | 代码位置 |
| :--- | :--- | :--- |
| **四档时效第 1 优先**（`0:<=2h, 1:2~6h, 2:6~12h, 3:12~24h`） | `delay_h` 与互动率显著负相关（`ALL rho=-0.339, p=0.0057`；`EN rho=-0.354, p=0.0089`），与原帖后续增长负相关（`EN rho=-0.381, p=0.0030`）；`<2h` 作者回复率达 `53.8%` vs `>=2h` `23.1%`（`EN mature p=0.0448`）。用四档替代二值 `over_2h` 可防止 `8~22h` 老帖反超 `2~5h` 新帖。 | `comment_publisher.py` `_age_bucket` / `_candidate_priority_key` |
| **同档内起势优先 + 回复数升序**（`likes>=1 or replies>=1` -> `replies` 升序 -> `-best_score`） | `C1~C3 (N=52)` 在该基线下稳定跑出 **`69.2%` 互动率**、`46.2%` 回复率、均阅 `23.1`（中位 `7.5`）。改为 `07837ef` 的 `(no_traction, over_2h, -views_per_hour)` 后，`C4 (N=16)` 互动率显著跌至 **`37.5%`（`p=0.0378`）**。 | `comment_publisher.py` `PUBLISH_STRATEGY_VERSION = "proven_v1_age4_traction_replies_asc"` |
| **安全上限 `max_post_comments = 15` + 回复数升序自然避挤** | 细拆 `N=68` 证明 `EN pr=4..9 (n=6)` 仍达 `66.7%~80%` 互动（均阅 `16.7`）、`JA pr=7..14` 仍可达 `10~52` 阅；`replies` 升序本身已在 `C1~C3` 自然把 `pr>=10` EN 控制在 `0/42`，无需一刀切砍到 `5` 损失供给。 | `growth_config.py` `PUBLISHER_DEFAULTS["max_post_comments"] = 15` |
| **库端不做静态冷死硬删，改由打开详情页实时复核**（DB `stale=0`，线上 `stale=7200`） | 此前库内快照静态过滤误杀率约 `13%`（线上已起赞）；`C3` 验证实时复核方案正常且互动 `70.0%`、回复 `60.0%`。 | `comment_publisher.py` `publish_comments_from_db` |
| **生成波次按 `publish_time` 降序处理 + `llm_timeout = 120`** | 第 12 轮网关单次卡死 `300.01s` 导致 9 帖波次拖延 `907.37s` 消耗 `<2h` 黄金窗口；缩至 `120s` 加快容灾切换，降序出队保障新帖优先获评。 | `comment_generator.py` / `growth_config.py` |
| **采集首页空采重载重试 1 次 + `empty_capture` 快速退避** | 修复 `00:50:01` 首页 `gql_ops={}` 却返回 `"ok"` 导致 `<2h` 断档 ~1.7h 的故障；两次无 GraphQL 响应显式告警并在 `<=600s` 内重采。 | `x_browser_collector.py` / `comment_pipeline.py` |

---

## 二、已否决 / 撤回的过度调整

1. **撤回 `(no_traction, over_2h, -views_per_hour)` 排序**：实测使 `C4` 互动率显著下滑（`69.2% -> 37.5%, p=0.0378`），且把 `8~22h` 老帖排进前列。
2. **撤回 `max_post_comments = 5` 一刀切硬砍**：`pr 4..9` 仍高表现，砍至 `5` 属看 `C4` 事后分箱过拟合。
3. **撤回 `likes >= 1` 排斥 `0 赞 1 评`**：`pl_pub >= 1` (`n=63`) vs `pl_pub == 0` (`n=5`) 在阅读与互动上无任何差异（`p=0.69~1.00`）。

---

## 三、待验证好点子台账（暂不动策略，由 `auto_comment.publish_telemetry` 持续埋点积累样本）

以下假设在 `N=68` 上尚缺控制混杂后的稳健证据，**现已将全部协变量写入每条已发评论的 `auto_comment.publish_telemetry`**，待基线新策略累计 `N >= 120+` 后统一回测裁决：

| 待验证点子 | 当前 `N=68` 观测状态（为何暂不上） | 已埋点字段（`auto_comment.publish_telemetry.*`） | 后续可落地判定标准 |
| :--- | :--- | :--- | :--- |
| **点子 A：在同新鲜度（`<2h`）与低回复内，按 `views_per_hour` 优先是否能放大单条曝光？** | `pv_vel` 强预测原帖继续增长（`rho=+0.676, p=0.0001`），但在 `EN <6h & pr<=9 (n=44)` 内对评论阅读 `p=0.241`、对互动率 `p=0.671` 均未达显著（`pv_vel>=150 & pr<=5` 仅 `n=8`）。 | `views_per_hour_at_publish`、`post_age_sec_at_publish`、`age_bucket`、`fresh_2h_pool_size` | 剔除 `C4` 混杂后，在 `proven_v1` 样本的 `<2h & pr<=5` 子集内若 `pv_vel` 对 `cv` 正相关达到 `p < 0.05` 且不显著拉低 `inter`，方可考虑作为同档二级微调测试。 |
| **点子 B：英语区 Quote（引用转发）原帖是否应降权或过滤？** | `EN Quote (n=12)` 互动率 `33.3%` vs 非 Quote `66.7%`（`p_int=0.0501` 仅卡临界线），但均值阅读 `18.5` 并不低于非 Quote `10.0`（`p_cv=0.35`）。 | `is_quote` | 当 Quote 样本累计 `>= 30` 条时，同时检验互动率与总阅读/净增粉贡献再定去留。 |
| **点子 C：目标作者粉丝区间是否应收窄至 `100~1499`？** | `EN mature` 中 `<1500` vs `>=1500` 中位阅读 `p=0.0343`，但在排除 `C4` 后的 `C1~C3` 内变为 `p=0.1744`，疑受 `C4` 高粉丝拥挤帖混杂。 | `author_followers`、`candidate_pool_size` | 控制 `strategy_version == "proven_v1_age4_traction_replies_asc"` 与 `pr_pub` 后若 `<1500` 仍显著优于 `>=1500` 且候选池不断供再考虑收窄。 |
| **点子 D：英语区拥挤真正塌陷拐点是否落在 `pr >= 10`（而非 `5`）？** | 目前 `EN pr >= 10` 仅 `n=6`（成熟 `n=4`），样本太少不足以设死门槛，且 `replies` 升序本身已在日常发评中优先避开。 | `max_post_comments_cap`、`candidate_rank`、`db_engagement_at_select` | 追踪当 `<2h` 仅剩 `pr=6~15` 帖时的实际表现，确认 `6~9` 与 `>=10` 在各语种的稳定分界线。 |
| **点子 E：图文帖 vs 纯文本帖在英/日区的差异** | 当前 `EN` 带图 vs 纯文差异未显著（`p_int=0.56, p_cv=0.70`）。 | `has_images`、`post_text_len` | 随同 `proven_v1` 样本自然累积至 `N>=150` 一并回归检验。 |
