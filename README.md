# observability-metrics-python

一套可在本地运行、结论可复现可解释的观测能力，基于 FastAPI（ASGI 中间件）实现：

- **请求级指标**：按 `route / method / status` 统计请求量与时延分布；路径参数与未匹配路由归一化，避免标签爆炸；异常请求不丢指标；并发下计数守恒。
- **窗口告警**：慢请求比例与错误率两类告警，支持滑动窗口、触发/恢复迟滞、冷却期，状态变化确定、可复现。
- **请求关联与异常链路**：每个请求生成或沿用 `X-Request-ID`，成功响应、失败响应、结构化 JSON 日志与异常处理共享同一标识。

## 目录结构

```
observability/
  metrics.py    # MetricsRegistry：计数器 + 累积直方图，线程安全
  alerts.py     # AlertRule / AlertEvaluator：滑动窗口 + 迟滞 + 冷却
  context.py    # RequestContextMiddleware + StructuredLogger：关联 ID
  middleware.py # ObservabilityMiddleware：串联指标/告警/日志，异常兜底
main.py         # 示例服务与默认告警规则
tests/          # 并发守恒、归一化、告警触发/恢复、异常路径测试
```

## 本地运行

```bash
uv sync
uv run fastapi dev main.py          # 或 uv run uvicorn main:app --port 8000
```

示例请求：

```bash
curl -i http://127.0.0.1:8000/                       # 成功（自动生成 request id）
curl -i -H 'X-Request-ID: trace-001' http://127.0.0.1:8000/users/42
curl -i http://127.0.0.1:8000/slow?ms=300            # 慢请求
curl -i http://127.0.0.1:8000/fail                   # 500 异常链路
curl -i http://127.0.0.1:8000/does/not/match         # 404 未匹配路由
curl -s http://127.0.0.1:8000/metrics | python -m json.tool
```

结构化日志以行分隔 JSON 打到 stderr，例如：

```json
{"event":"request.start","level":"info","method":"GET","path":"/fail","request_id":"9c84…","request_id_reused":false}
{"event":"request.complete","level":"info","method":"GET","route":"/fail","status":500,"duration_ms":0.35,"request_id":"9c84…","alerts":[]}
{"event":"request.exception","level":"error","elapsed_ms":0.43,"error_type":"unhandled_exception","request_id":"9c84…"}
```

## 指标语义

`GET /metrics` 返回 `MetricsRegistry.snapshot()`：

- `total_requests`：进程启动后记录的请求总数（**每个 HTTP 请求恰好记录一次**，包括成功、4xx、5xx 与未捕获异常）。
- `series[]`：按标签组合聚合的序列：
  - `route` / `method` / `status`：三维标签；
  - `count`：该序列请求数；`sum_ms` / `avg_ms`：时延总和与均值；
  - `buckets`：时延累积直方图（毫秒），上界为
    `5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000`，另含 `+Inf`；
    `buckets[b]` 表示时延 `<= b` 的请求数，因此桶计数单调不减。
- **守恒不变量**：`total_requests == Σ series[].count`。计数器与总数在同一把锁内更新，并发下恒成立（测试以 32 线程 × 200 请求断言）。
- **错误判定**：HTTP `status >= 500` 或业务未捕获异常记为错误；4xx 不计入错误率分子，但仍计入请求总数。

### 标签归一化规则（防标签爆炸）

| 情况 | 原始路径 | `route` 标签 |
| --- | --- | --- |
| 匹配到路由 | `/users/42`、`/users/999` | `/users/{user_id}`（路由模板） |
| 未匹配路由 | `/random/1`、`/random/2`、任意路径 | `unmatched`（单一标签） |
| 业务异常 | `/fail` | `/fail` + `status=500` |

规则：匹配成功一律使用路由模板（路径参数天然收敛）；未匹配一律归并为常量 `unmatched`，与具体路径无关。

## 告警窗口、阈值与冷却规则

`AlertRule` 字段（单位均为毫秒）：

| 字段 | 含义 |
| --- | --- |
| `window_ms` | 滑动窗口长度；判定时刻保留 `(now - window_ms, now]` 的样本，**恰好在左边界上的样本保留**，下一个毫秒淘汰 |
| `threshold` | 触发阈值：窗口内命中比例 `>= threshold` 触发（比例 = 慢请求或错误请求数 / 窗口样本数） |
| `recover_threshold` | 恢复阈值（迟滞）：比例 `< recover_threshold` 才解除；与触发阈值之间形成迟滞带 |
| `match_threshold` | 仅慢请求告警使用：单请求时延 `>= match_threshold` 计为慢请求 |
| `min_samples` | 窗口内至少多少样本才允许触发；FIRING 状态下样本数跌破该值则确定解除 |
| `cooldown_ms` | 冷却期：一次状态变化后，`cooldown_ms` 内抑制所有状态变化事件 |

状态机：

```
            样本数>=min_samples 且 ratio>=threshold（且非冷却期）
  OK ───────────────────────────────────────────────────────────► FIRING
  ◄───────────────────────────────────────────────────────────
     样本数>=min_samples 且 ratio<recover_threshold（且非冷却期）
     或 样本数<min_samples（且非冷却期）
```

确定性与抗抖动保证：

1. **重启初始状态恒为 `OK`**，无历史样本、不会凭空调用；
2. 触发与恢复采用不同阈值（迟滞），临界值附近的小幅抖动不会反复翻转；
3. **冷却期内同一状态至多发生一次变化**——即便条件已满足，变化也只记录为 `suppressed_by_cooldown`，不产生事件；冷却结束后下一次真实变化才生效；
4. 每次判定都会通过标准日志输出输入与结论：
   `input(now, samples, ratio, threshold, recover, min_samples, state, cooldown) -> conclusion(fired|resolved|hold|suppressed_by_cooldown)`，
   便于复现每次判定依据（测试中以 `caplog` 断言该日志存在）。

默认规则（`main.py`）：两类告警均为 10 秒窗口、`min_samples=5`、触发 50%、恢复 25%、冷却 5 秒；慢请求时延阈值 200ms。

## 请求关联标识规则

- 请求头 `X-Request-ID` 合法时**沿用**：长度 1–128、可打印 ASCII（33–126）、不含空白与控制字符。
- 缺失/为空/**非法或冲突**时确定地**丢弃外部值并重新生成**（`uuid4().hex`），同时输出 `request.id_invalid` 告警日志，服务不报错。
- 逗号拼接的多值 ID 只取第一个非空片段；第一片段非法即重新生成（不回退到后续片段）。
- 最终生效的 ID 会：写入 `contextvars`（业务代码用 `get_request_id()` 读取）、回写响应头 `X-Request-ID`（成功与 500 失败响应一致）、贯穿 `request.start` / `request.complete` / `request.exception` 全部结构化日志。

## 本地验证

```bash
uv run pytest -s                                  # 全部测试（-s 可查看每次告警判定日志）
uv run pytest tests/test_metrics.py -q            # 并发守恒 / 归一化 / 直方图
uv run pytest tests/test_alerts.py -q             # 触发 / 迟滞 / 冷却 / 窗口边界
uv run pytest tests/test_http_e2e.py -q           # 端到端：异常链路 / 告警全生命周期
```

覆盖说明：

- **并发请求**：32 线程 × 200 请求断言 `total_requests` 与各维度计数总和守恒；
- **路由归一化**：路径参数收敛到模板、500 条任意未匹配路径只产生一个 `unmatched` 序列；
- **告警触发与恢复**：可控虚拟时钟驱动，断言 OK→FIRING→OK 转换、冷却抑制与窗口边界样本保留；
- **异常路径**：`/fail` 抛异常后仍返回 500 且响应头、日志、指标共享同一 request id，指标不缺失。
