# Observability Metrics Python

一个可本地运行的 FastAPI 观测能力示例：请求级指标、窗口阈值告警、请求关联与异常链路。

## 快速开始

```bash
uv sync                 # 安装依赖
uv run uvicorn main:app # 启动服务（默认 :8000）
uv run pytest           # 运行全部测试（含并发与异常路径）
```

本地验证：

```bash
curl -i localhost:8000/                       # 正常请求，响应头含 X-Correlation-ID
curl -i localhost:8000/items/42               # 路径参数请求
curl -i localhost:8000/boom                   # 触发异常的请求（500，仍计入指标）
curl -i localhost:8000/no/such/path           # 未匹配路由（404，归一化为 __unmatched__）
curl -s localhost:8000/metrics | jq           # 指标快照（含近似 p50/p95/p99）
curl -s localhost:8000/alerts  | jq           # 告警状态（重启后恒为 ok）
curl -s 'localhost:8000/alerts/error_rate/history?limit=20' | jq
                                             # 最近判定记录（输入/结论/转换/冷却）
```

## 指标语义

每个请求（包括抛出异常与未匹配路由的请求）恰好记录到 **一个** 标签序列
`(route, method, status_code)`，因此所有序列的 `count` 之和恒等于请求总数
（守恒性由并发测试断言）。每个序列包含：

- `count`：请求数；
- `latency_sum`：时延总和（秒）；
- `latency_buckets`：时延直方图，桶上界为
  `0.005 / 0.01 / 0.05 / 0.1 / 0.5 / 1.0 / 5.0` 秒，外加 `+Inf` 桶；
  观测值落入第一个满足 `latency <= 上界` 的桶。**边界归属**：观测值
  恰好等于某个桶上界时，计入“以上界命名”的那个桶（例如 `0.01` 计入
  上界为 `0.01` 的桶）；只有严格大于 `5.0` 的值才进入 `+Inf`；零值
  与负值归一到第一个桶；
- `latency_quantiles`：基于直方图的近似分位数，默认给出 `p50 / p95
  / p99`，方便值班快速判断慢到什么程度。近似口径为**上界法 + 命中桶内
  线性插值**：目标秩为 `q * count`，从左向右累加桶计数找到第一个累计
  计数 `>=` 目标秩的桶，再按桶内已消耗比例在该桶的上下界之间线性插值；
  若目标秩落入 `+Inf` 桶则没有有限上界，JSON 中输出 `null`（含义是
  “高于最大有限上界”）；无观测时同样为 `null`。误差不超过命中桶的
  桶宽，因此适合快速判断，不应当作精确分位数使用。

`GET /metrics` 返回 `total_requests`、所用的 `quantiles` 列表与全部
序列；`total_requests` 恒等于各序列 `count` 之和，且每个序列各桶计数
之和恒等于该序列 `count`（边界值、零值、超大值与大批观测并发写入时均
成立，由测试断言）。

## 标签归一化规则

为避免标签爆炸，`route` 标签按以下确定规则归一化：

1. 请求路径与某个已注册路由模板**完全一致**时，使用该模板
   （如 `/items/42` → `/items/{item_id}`）；
2. 多个模板可匹配时，按“字面量段数最多、其次字典序最小”取第一个；
3. 没有任何模板匹配时，统一使用 `__unmatched__`，任意路径不会扩散标签基数。

## 告警窗口、最小样本、双门槛与冷却规则

每条告警规则（`slow_request_rate`、`error_rate`，以及以后新增的规则）
都走同一套 `WindowedAlert` 判定管线，以下约定对所有规则统一生效：

- **窗口**：覆盖 `(now - window_seconds, now]`，早于等于边界的观测被逐出；
- **取值**：窗口内“违规观测数 / 窗口内观测总数”；
- **最小样本**：窗口内样本数少于 `min_samples` 时判定结果为“样本不足”
  （`insufficient_samples=true`）：维持当前状态、不触发转换，也不会被
  记为冷却抑制（冷却不会在样本不足期间被白白消耗）。样本攒够后才按
  门槛判定，避免窗口里只有一两个样本时被抖动带偏；
- **触发/恢复双门槛与迟滞区间**：触发与恢复使用两条不同的线——
  - 当前为 `ok` 时，仅当 `value >= fire_threshold`（更高的线）才触发；
  - 当前为 `firing` 时，仅当 `value < recover_threshold`（更低的线）
    才恢复（注意恢复是严格小于，恰好在恢复线上不恢复）；
  - `recover_threshold <= value < fire_threshold` 的中间迟滞区间内
    维持当前状态，不允许来回翻；
- **冷却**：一次状态转换后 `cooldown_seconds` 内不允许再次转换；
  冷却期内的转换需求被**抑制**（状态保持、需求丢弃，
  `suppressed_by_cooldown=true`），因此短时抖动不会造成反复触发，且
  同一告警在冷却期内最多发生一次状态变化；
- **初始状态**：进程重启后所有告警恒为 `ok`，行为确定。

`min_samples`、`fire_threshold`、`recover_threshold` 均可按规则单独配置
（`AlertConfig` 字段）。`AlertConfig.threshold` 作为兼容写法保留：未显式
给出双门槛时，触发线与恢复线都取它，恢复成旧的单线语义。

每次判定都会输出一条结构化日志（logger `observability.alerting`，事件
名 `alert_evaluation`），消息体本身就汇总了本次判定的输入与结论，JSON
字段则包含：输入（`timestamp`、`violated`、`window_size`、`min_samples`、
`fire_threshold`、`recover_threshold`）与结论（`value`、`previous`、
`current`、`transitioned`、`suppressed_by_cooldown`、
`insufficient_samples`，以及单调递增的 `sequence`）。这些字段直接出现在
服务自己往 stdout 打的 JSON 行里，不只是留在进程内存中。

## 判定记录查询

`GET /alerts/{name}/history?limit=50` 是只读入口，按某条告警查看最近一段
判定记录（未知告警名返回 `404`，`limit` 取值 `1..1024`，默认 `50`）：

```bash
curl -s 'localhost:8000/alerts/error_rate/history?limit=20' | jq
```

返回体含当前 `state` 与 `records`（**最新在前**），每条记录即一次判定：
输入（时间戳、是否违规、窗口样本数、最小样本与两条门槛）、结论（前值/现值、
是否真的发生了状态转换、是否被冷却拦下、是否因样本不足而未下结论）。状态与
历史在同一把锁下原子快照，因此返回的 `state` 必与最新一条记录的 `current`
一致；记录按 `sequence` 连续且不重，历史有界（默认每条规则保留最近 1024 条，
超出后最旧记录被逐出），并发读取不会丢记录也不会读到重复。

## 请求关联与异常链路

- 每个请求解析 `X-Correlation-ID` 请求头：合法值（非空、≤128 字符、
  可打印 ASCII 且无空白）原样沿用；缺失或非法值一律替换为新生成的
  uuid4 hex——缺失、冲突、非法输入适用同一条确定规则；
- 响应头始终回写 `X-Correlation-ID`；成功响应、失败响应（异常兜底 500 的
  响应体同样携带）、结构化日志（`correlation_id` 字段）共享同一标识；
- 异常发生时：异常事件日志（`request_exception`）、完成日志
  （`request completed`）与指标记录都不会缺失。

## 测试

```bash
uv run pytest -v
```

覆盖：路由归一化、并发下计数守恒（线程池 + asyncio 并发）、分桶边界归属
（上界值、零值、负值、超大值）与大批观测下的桶计数守恒、近似分位数
（插值、`+Inf`、无观测）、告警最小样本门限、触发/恢复双门槛与迟滞区间、
按规则独立配置门槛、触发/恢复/冷却抑制/窗口逐出、判定记录查询的一致性
（状态与最新记录一致、不丢不重、有界、只读）、重启初始状态、异常路径的
指标与日志完整性、关联标识的沿用与替换规则。`pytest` 已开启
`--log-cli-level=INFO` 且日志格式只取消息体，因此跑测试时每条
`alert evaluation` 的输入与结论都会直接打印出来。

## 兼容范围

对外行为尽量保持不变，以下是需要留意的点：

- **默认行为不变**：`min_samples` 默认为 `1`；不显式给出双门槛时，触发线
  与恢复线都取原 `threshold`（默认 `0.2`），因此旧调用
  （`AlertConfig(name, window_seconds, threshold, cooldown_seconds)` 位置
  参数）与既有告警语义保持一致；`/metrics`、`/alerts` 的原有字段不变，
  新增字段（`quantiles`、序列上的 `latency_quantiles`）都是附加的；
- **行为修正**：分桶边界由实现层面的“严格小于上界才入桶”改为文档所述的
  “`<= 上界` 即入以上界命名的桶”，恰好落在上界上的观测会从前一个桶移动
  到以上界命名的桶（各桶计数之和与请求总量守恒不变）；
- **日志**：`alert evaluation` 的消息文本不再是裸字符串，而是内嵌了输入
  与结论的可读摘要；结构化字段仍可从日志记录上直接读取，同时进入 JSON 行；
- 新接口 `GET /alerts/{name}/history` 为纯只读新增，不改变任何既有路由。
