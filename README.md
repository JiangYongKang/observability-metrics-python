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
curl -s localhost:8000/metrics | jq           # 指标快照
curl -s localhost:8000/alerts  | jq           # 告警状态（重启后恒为 ok）
```

## 指标语义

每个请求（包括抛出异常与未匹配路由的请求）恰好记录到 **一个** 标签序列
`(route, method, status_code)`，因此所有序列的 `count` 之和恒等于请求总数
（守恒性由并发测试断言）。每个序列包含：

- `count`：请求数；
- `latency_sum`：时延总和（秒）；
- `latency_buckets`：时延直方图，桶上界为
  `0.005 / 0.01 / 0.05 / 0.1 / 0.5 / 1.0 / 5.0` 秒，外加 `+Inf` 桶；
  观测值落入第一个满足 `latency <= 上界` 的桶。

`GET /metrics` 返回 `total_requests` 与全部序列；`total_requests` 恒等于
各序列 `count` 之和。

## 标签归一化规则

为避免标签爆炸，`route` 标签按以下确定规则归一化：

1. 请求路径与某个已注册路由模板**完全一致**时，使用该模板
   （如 `/items/42` → `/items/{item_id}`）；
2. 多个模板可匹配时，按“字面量段数最多、其次字典序最小”取第一个；
3. 没有任何模板匹配时，统一使用 `__unmatched__`，任意路径不会扩散标签基数。

## 告警窗口与冷却规则

每条告警规则（`slow_request_rate`、`error_rate`）维护一个滑动窗口：

- **窗口**：覆盖 `(now - window_seconds, now]`，早于等于边界的观测被逐出；
- **取值**：窗口内“违规观测数 / 窗口内观测总数”；
- **触发**：`value >= threshold` 时进入 `firing`；
- **恢复**：`value < threshold` 时回到 `ok`；
- **冷却**：一次状态转换后 `cooldown_seconds` 内不允许再次转换；
  冷却期内的转换需求被**抑制**（状态保持、需求丢弃），因此短时抖动不会
  造成反复触发，且同一告警在冷却期内最多发生一次状态变化；
- **初始状态**：进程重启后所有告警恒为 `ok`，行为确定。

每次判定都会输出一条结构化日志（`alert evaluation`），包含输入
（时间戳、是否违规、窗口大小、阈值）与结论（前值、现值、是否转换、是否被冷却抑制）。

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

覆盖：路由归一化、并发下计数守恒（线程池 + asyncio 并发）、告警触发/恢复/
冷却抑制/窗口逐出、重启初始状态、异常路径的指标与日志完整性、关联标识的
沿用与替换规则。单测日志中可看到每次告警判定的输入与结论。
