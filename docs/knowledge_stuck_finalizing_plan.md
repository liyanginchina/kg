# 知识文件卡在「优化中」(finalizing) 根因分析与修复计划

> 目标知识库：**集团AI审计知识库 RAG+Wiki**（`4dc5bbcf-1327-432a-a676-7b1b45bad883`）
> 现象：绝大多数文件进度持续卡在「优化中」，仅少量（20/339）完成。
> 日期：2026-07-28

---

## 实施状态（2026-07-28 更新）

### P0 — graph 工作池并发 8→32 ✅ 已上线
- **代码**：`internal/types/task.go` 的 `DefaultGraphWorkerConcurrency` 由 `8` → `32`。
- **环境变量（持久化）**：`.env` 新增 `WEKNORA_ASYNQ_GRAPH_CONCURRENCY=32`；`docker-compose.yml` 的 app `environment:` 新增对应行（默认 `8`，被 env 覆盖）。
- **热生效验证**：重建 `WeKnora-app` 容器后，日志确认
  `asynq graph-pool server starting with concurrency=32`，`/health` 返回 `{"status":"ok"}`。
- **回退**：删除 `.env` 中该行（或置 `WEKNORA_ASYNQ_GRAPH_CONCURRENCY=8`）并 `docker compose up -d app` 即回退。

### P1 — graph 抽取批处理 ✅ 代码完成并验证，镜像构建中
- **代码改动**：
  - `internal/types/task.go`：`ExtractChunkPayload` 新增 `ChunkIDs []string` 与 `BatchIndex int`（批处理模式；旧单 chunk 任务仍兼容）。
  - `internal/application/service/extract.go`：新增 `NewChunkExtractBatchTask`（一任务携带一个 chunk id 窗口）；`ChunkExtractService.Handle` 重构为支持批处理循环，每任务终态仅 `FinalizeSubtask` 一次；抽取逻辑抽为 `extractGraphForChunk`。
  - `internal/application/service/knowledge_post_process.go`：新增 `graphGenChunkBatchSize=20` 与 `graphBatchCount()`；图谱扇出改为按窗口批入队，`expectedSubtasks` 与 shortfall 对账改用 `graphBatchCount`（不再用 `graphChunkCount`）。
- **测试**：新增 `TestGraphBatchCount`（含边界）+ `TestGraphBatchCountMatchesEnqueueLoop`（种子数==入队批数），以及 repository 包 `TestFinalizeSubtask_GraphBatchCountPromotesExactlyOnce`（并发下恰好一次 promote）。
- **验证**：`go build ./internal/...` 与 `go vet` 通过；上述单测在 Docker(golang:1.26-bookworm, CGO) 下全部 PASS。
- **部署**：app 镜像正在后台重建（`docker compose build app`），完成后 `docker compose up -d app` 即生效；已入队的旧单 chunk 任务仍被兼容执行，新任务转为批处理。

### 备注
- P0 是吞吐主杠杆（已即时生效）；P1 主要降低 asynq 队列深度与 `FinalizeSubtask` DB 竞争，结构性消除瓶颈，二者叠加后 backlog 将快速清空，572 个「优化中」行会随 graph 任务完成自然 `completed`，无需手动改状态。

---

## 一、根因分析（已用线上数据确认）

### 1.1 状态机回顾
知识文件的处理状态机（`ParseStatus`）：
```
pending → processing → finalizing → completed / failed / cancelled / deleting
```
「优化中」= `ParseStatusFinalizing`（见 `frontend/src/i18n/locales/zh-CN.ts:641,693`）。

`finalizing` 是**异步富集扇出**阶段：post-process 编排器（`knowledge_post_process.go` `Handle`）把
summary / question / graph / wiki 子任务入队，并把 `pending_subtasks_count` 设为子任务总数。
每个子任务在**终态退出时**调用 `FinalizeSubtask` 原子地 `-1`；计数归零且状态仍为 `finalizing` 时，
同一条 `UPDATE` 把行提升为 `completed`（`knowledge.go:421-490`）。

**结论：行卡在 `finalizing` 当且仅当 `pending_subtasks_count` 永远到不了 0。**

### 1.2 线上证据
| 观测 | 数据 | 含义 |
|------|------|------|
| 目标 KB 状态分布 | `finalizing=304, completed=20, deleting=15` | 绝大多数卡在 finalizing |
| finalizing 行的 `pending_subtasks_count` | `320 / 285 / 201 / 182 / …` | 余量恰等于**该文档文本 chunk 数** → 仅 graph（每 chunk 一个任务）未释放 |
| 全库 finalizing 行数 | `572` | **系统性瓶颈**，非单 KB |
| 目标 KB 剩余子任务槽合计 | `8623`（303 行） | ≈ Redis 中 graph 待处理任务量 |
| Redis `asynq:{graph}:pending` | `8506` | 8506 个 graph 任务排队 |
| Redis `asynq:{graph}:active` | `8` | 仅 8 个在跑（= graph 池并发） |
| Redis `asynq:{graph}:archived` | `9999`（封顶） | ~1万已完成，任务**在成功推进** |
| Redis `asynq:retry:*` / `asynq:dead:*` | 空 | **无失败/无重试** → 不是错误导致卡死 |
| 20s 采样吞吐 | pending `8508→8506`，完成 ~2 个 | ≈ **6 任务/分钟** |
| `NEO4J_ENABLE` | `true` | graph 任务确实被入队 |
| housekeeping `filterOutQueued` | 跳过「仍有排队任务」的行 | 正确地把 backlog 当作背压，**不回收**这些行 |

### 1.3 根因结论
**graph（图谱抽取）是吞吐量瓶颈，而非逻辑死循环或状态未更新缺陷。**

1. `NewChunkExtractTask`（`extract.go:99`）在 `NEO4J_ENABLE=true` 时，对**每个文本 chunk 生成 1 个 graph 任务**
   （`knowledge_post_process.go:205-208, 330-342`：`graphChunkCount = len(textChunks)`）。
   一个 304 文档的 KB → 数万个 LLM 往返。
2. graph 任务被独立隔离到 `WorkerPoolGraph`，**默认并发仅 8**（`internal/types/task.go`，上一轮 D/E/F 优化引入）。
   - 这是一次**回归**：graph 原本在共享 enrichment 池（并发 12），隔离后降到 8，吞吐反而下降。
3. 每个 graph 任务 = 一次图谱实体抽取 LLM 调用（经 `api.chinalo.com.cn` DeepSeek 网关），实测单次 **~80s**。
   - 8 并发 × 80s ≈ 6 任务/分钟。
4. 上一轮 A/B/C 修复使 graph 任务从「失败」变为「成功」，于是任务真正入队流动，
   但**暴露了长期存在的吞吐上限**；叠加 E 把并发从 12 降到 8，backlog 显现为「卡在优化中」。

> 注：这与「RAG 分块/向量化异常」「索引服务超时」「finalizing 死循环」无关——
> 分块与向量化在 `processing` 阶段已完成（否则不会进入 finalizing）；retry/dead 为空证明 graph 任务在成功推进，只是太慢。

---

## 二、修复步骤（按收益/风险排序）

### P0 — 提高 graph 工作池并发（立即生效，最低风险，最高性价比）
- **改动**：`internal/types/task.go` 中 `DefaultGraphWorkerConcurrency` 由 `8` → `32`（或直接更高，如 48）。
  该值可通过环境变量 `WEKNORA_ASYNQ_GRAPH_CONCURRENCY` 覆盖（已具备）。
- **原理**：graph 任务是 I/O 密集型（等待 LLM），高并发安全。并发 8→32 ≈ 吞吐 4×，backlog 清空时间 24h → ~6h。
- **可热生效**：不依赖代码发布也可临时 `export WEKNORA_ASYNQ_GRAPH_CONCURRENCY=32` 后重启 app 验证。

### P1 — graph 抽取批处理（结构性，任务数降 ~20×）
- **改动**：仿照 question generation 的 `questionGenChunkBatchSize=20`（`knowledge_post_process.go:467`），
  将「每 chunk 一个任务」改为「每 N 个 chunk 一个批任务」：
  - `knowledge_post_process.go` 循环改为按 `graphGenChunkBatchSize` 分批入队 `TypeChunkExtract`，
    每个任务携带 chunk id 列表。
  - 计数器口径：把 `expectedSubtasks` 中 `graphChunkCount` 改为 `graphBatchCount`。
  - 批任务 handler 内对每个 chunk 依次抽取，终态 `defer` 仍只调用 `FinalizeSubtask` 一次（每批释放 1 槽）。
- **原理**：减少 asynq 调度、DB finalize、上下文切换开销，单 worker 每调度周期做更多有效工作。
  总 LLM 工作量不变，但吞吐上限提升且 Redis 队列深度骤降（8506 → ~425）。
- **风险**：计数口径必须匹配入队批数，否则行卡死或提前完成。已有 `shortfall` 释放兜底（`knowledge_post_process.go:383-408`）+ 单测保障。

### P2 — 降低单任务 LLM 时延
- **改动**：
  - 排查 graph 抽取 prompt 是否过重；评估改用更轻/更快的模型（与 summary 模型解耦）。
  - 将单任务 `asynq.Timeout(30*time.Minute)`（`extract.go:117`）收紧为更合理的上限（如 10min），
    配合已有的瞬时重试，使偶发挂起的任务更快被回收。
  - 核查 `api.chinalo.com.cn` DeepSeek 网关的速率限制，在允许范围内提高并发调用数。

### P3 — 复核 E 隔离策略
- 隔离意图（graph 不被向量化挤占）正确，但默认并发必须匹配实际负载。保留隔离，默认并发提到 32+。
- （进阶，可选）在 graph 池低负载时允许借用 shared 池容量。

### P4 — 已卡住行的回收
- **无需手动改状态**：当前 572 个 finalizing 行是「合法 finalizing」，P0/P1 上线后 backlog 自然清空即自动 `completed`。
- housekeeping 已能在「无排队任务且超过阈值」时回收（当前因任务仍在排队，正确地不回收）。
- 上线后只需**监控** graph pending 深度下降、finalizing 计数下降、completed 计数上升。

---

## 三、验证方式

1. **单元/集成测试**
   - 复用 `knowledge_finalize_test.go` 风格，新增「批处理计数口径」测试：
     验证 `SetFinalizing` 种子数 == 实际入队批数，`FinalizeSubtask` 在批任务终态精确 -1，
     并发下恰好一次 promote 到 completed（`TestFinalizeSubtask_Concurrent_ExactlyOnePromote` 同思路）。
   - `go build ./internal/...` 全量编译（走带 `libsqlite3-dev` 的 Docker 镜像）。

2. **部署后线上验证**
   - `curl -s http://127.0.0.1:8080/health` → `{"status":"ok"}`。
   - Redis 采样：`asynq:{graph}:pending` 持续下降；`active` 随并发上升（→32）。
   - DB 采样：`knowledges WHERE parse_status='finalizing'` 行数下降、`completed` 上升。
   - 应用日志：graph-pool `concurrency=32` 启动；`GRAPH_EXTRACT_FAILED` 无新增激增。
   - 目标 KB：持续观察 `finalizing` 304 → 逐步归零。

3. **回归**
   - 确认无新行文「永久 finalizing」；确认 summary/question/wiki 子任务不受影响。

---

## 四、风险评估

| 措施 | 风险 | 缓解 | 可逆性 |
|------|------|------|--------|
| P0 提高 graph 并发 8→32 | 瞬时打满 LLM 网关 → 429 限流、重试增多 | graph 写入幂等（`apoc.merge`）；已有 `MaxRetry(3)` + 瞬时重试；网关自身限流 | 高（常量/环境变量，重启即回退） |
| P1 graph 批处理 | 计数口径不匹配 → 行卡死或提前完成 | 已有 shortfall 释放兜底 + 新增单测 | 中（需重新发版） |
| P2 收紧超时/换模型 | 误杀慢但有效的任务 | 超时设 10min（<<30min）；换模型前灰度 | 高 |
| P3 隔离策略 | 无负面（仅提并发） | — | 高 |
| P4 不手动改状态 | 若误将 finalizing 标 completed → 图谱缺失 | 坚持「让其自然排空」，仅监控 | — |

**数据安全**：graph 写入为幂等 upsert，提高并发不会重复或丢失；不涉及 chunk/content 变更。

---

## 五、优先级建议
1. 先上 **P0**（一行常量/环境变量，零逻辑风险）→ 立即把吞吐提升数倍，backlog 开始快速下降。
2. 并行准备 **P1**（批处理）并入下一发版，从结构上消除瓶颈。
3. **P2/P3** 视 LLM 网关实测速率择机推进。

> 说明：本瓶颈部分由上轮 D/E/F 优化中「graph 独立池默认并发 8（低于原共享池 12）」引入，属回归，
> 故 P0 实质是回补该容量。
