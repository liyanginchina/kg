# 知识图谱构建性能与 `GRAPH_EXTRACT_FAILED` 根因分析 + 优化方案

> 分析对象：`postprocess.wiki` 模块（wiki 入库管线 `wiki_ingest*.go`）与知识图谱抽取任务（`extract.go` 的 `ChunkExtractService`，错误码 `GRAPH_EXTRACT_FAILED`）。
> 结论先行：**已实施 3 项代码修复（A/B/C），另有 3 项性能优化方案（D/E/F）建议落地。**

---

## 一、根因定位

### A. `postprocess.wiki` 的 JSON 容错远弱于图谱抽取器（易出错的根因）
- 文件：`internal/application/service/wiki_ingest.go:2832` `cleanLLMJSON`
- 现状：`cleanLLMJSON` 只做「去 ``` 围栏 + 转义字符串内控制字符」。它**没有**复用 `chatpipeline` 中图谱抽取器 `ParseGraph` 已经验证过的 `repairJSON` / `salvageFragments` 容错逻辑。
- 后果：DeepSeek / 中铝网关（chinalco） notoriously 返回的非严格 JSON（字符串内未转义换行/制表符、结尾多余字符、缺失逗号、多个对象拼接、结构位混入非 ASCII mojibake）会直接让 `json.Unmarshal` 失败 → Pass 0 报错 → 回退到 legacy 抽取器（用的仍是同一个弱 `cleanLLMJSON`）→ 也失败 → `EXTRACT_FAILED`。结果：该文档 wiki 页未被创建，上一轮修复的「知识图谱视图」缺节点。
- 证据：`wiki_ingest_cite.go:108`（`extractCandidateSlugs`）与 `wiki_ingest_cite.go:316`（`classifyChunkCitations`）在 `cleanLLMJSON` 之后仅做一次严格 `json.Unmarshal`，失败即整体报错。

### B. 空内容 chunk 直接触发 `GRAPH_EXTRACT_FAILED`（边界条件缺陷）
- 文件：`internal/application/service/extract.go` `ChunkExtractService.Handle`（约 276–337 行）
- 现状：`extractor.Extract(ctx, chunk.Content)` 之前**没有空内容守卫**。当 chunk 内容为空串或仅空白（OCR-only 页、纯图片 chunk、预处理被剥离文本的文档）时，模型拿到空 prompt 返回空/散文 → `ParseGraph`/`parseOutput` 报 `empty or invalid input string` → 整个 chunk 标记 `GRAPH_EXTRACT_FAILED`。
- 后果：图片型 PDF、扫描件极为常见，整本文档可能被少数空 chunk 拖垮。

### C. 图谱 LLM 调用无瞬时重试（稳定性缺陷）
- 文件：`extract.go` `ChunkExtractService.Handle`
- 现状：`extractor.Extract` 只调用一次。网关偶发 5xx/504/timeout 时立即返回错误 → 本次 attempt 即 `GRAPH_EXTRACT_FAILED`。虽然 asynq 有 `MaxRetry(3)`（`extract.go:117`），但默认退避很长（≈10s/40s/90s）且会重跑整个任务。
- 后果：网关抖动被放大成任务失败，吞吐下降。

### D. 每 chunk 一次 Neo4j 写事务（性能瓶颈 · 大批量）
- 文件：`internal/application/repository/retriever/neo4j/repository.go:60` `addGraph`
- 现状：每个 chunk 的图写入都 `driver.NewSession` + `ExecuteWrite` 开**一个全新事务**，内部 `UNWIND` 仅覆盖该 chunk 的极少数节点/关系。KB 有上万 chunk 时，图谱抽取扇出成**上万次微型 Neo4j 写事务**——会话/连接 churn 与每事务开销主导延迟；并发写同一 `kg` 还会在 Neo4j 写锁上竞争 → 瞬时 `addGraph` 错误 → 又触发 `GRAPH_EXTRACT_FAILED` 重试。
- 关键点：写入本身是幂等的（`apoc.merge.node` / `apoc.merge.relationship`，按 `name+kg` upsert），所以**合并批写是安全的**。

### E. 图谱抽取共享 Enrichment Worker 池（吞吐被饿死）
- 文件：`internal/types/task.go:75` `QueueGraph` 归入 `WorkerPoolEnrichment`，`Weight:1, SharedWeight:1`
- 现状：图谱抽取（TypeChunkExtract）与向量化等 enrichment 任务抢同一个共享池。批量入库时图谱抽取被饿死 → 整体「解析效率极低」。

### F. `postprocess.wiki` 多轮全量 LLM 调用（性能 · 大文档）
- 文件：`wiki_ingest_batch.go` `mapOneDocument`（约 1183–1430 行）
- 现状：单个文档最多 3 轮 LLM 调用都携带**完整重建内容**（上限 `maxContentForWiki=32768` 字符）：Pass 0（候选 slug）、Summary、Classify（Classify 还把全部 chunk 文本经 `renderChunksXML` 重新下发，`maxRunesPerCitationBatch=12000`）。大文档成本很高。

---

## 二、已实施的代码修复（A / B / C）

| # | 文件 | 改动 | 解决 |
|---|------|------|------|
| A1 | `chat_pipeline/extract_entity.go` | 新增导出 `RepairJSON` / `SalvageJSONToMaps`，复用既有的 `repairJSON`/`salvageFragments` | 共享同一套经实战验证的 LLM-JSON 容错 |
| A2 | `wiki_ingest.go` `cleanLLMJSON` | 非法 JSON 时调用 `chatpipeline.RepairJSON` 修复；新增 `salvageInto` 兜底 | 单条坏数据不再拖垮整篇抽取 |
| A3 | `wiki_ingest_cite.go` `extractCandidateSlugs` / `classifyChunkCitations` | 解析失败先 `salvageInto` 抢救片段，再决定整体失败 | `EXTRACT_FAILED` 显著减少 |
| B | `extract.go` `ChunkExtractService.Handle` | `extractor.Extract` 前增加 `strings.TrimSpace(chunk.Content)==""` 守卫，跳过并标记 `skipped=empty_content` | 空 chunk 不再产生 `GRAPH_EXTRACT_FAILED` |
| C | `extract.go` `ChunkExtractService.Handle` | `extractor.Extract` 包 3 次瞬时感知重试（2s/4s 退避，`isTransientLLMError` 判定），非瞬时错误立即返回 | 网关抖动不再直接失败任务 |

---

## 三、性能优化方案（D / E / F，建议落地）

### D. Neo4j 写入批量化（最高收益）
- 在 `ChunkExtractService` 内对**同一 knowledge** 的多个 chunk 图做本地缓冲（如每 50 个 chunk 或每 2s flush 一次），合并为一次 `AddGraph([]*GraphData{...})`。
- 同步将 `addGraph` 改为**单 session / 单事务**内顺序执行多个 `UNWIND`（当前已是 per-graph 循环，只需把 `NewSession`/`ExecuteWrite` 提到外层）。
- 收益：写事务数从「chunk 数」降到「批次数」（约 1–2 个数量级），消除会话 churn 与写锁竞争，`addGraph` 瞬时错误下降。

### E. 图谱抽取独立/提权队列
- 选项 1：把 `QueueGraph` 从 `WorkerPoolEnrichment` 移到独立 `WorkerPoolGraph`（参照 `WorkerPoolPostProcess` 模式），调高 `asynq.enrichment_concurrency` / 新增 `asynq.graph_concurrency` 环境变量。
- 选项 2（最小改动）：直接调大 `WorkerPoolEnrichment` 并发上限。
- 收益：批量入库时图谱抽取不再被向量化饿死。

### F. 减少 wiki 多轮全量 LLM 成本
- Pass 0 已产出候选 slug，Summary 阶段可复用同一份 `content` 且无需再次全量下发（候选列表已带 description）；考虑将 Summary 与 Pass 0 合并为单调用。
- Classify 的 `maxRunesPerCitationBatch` 在大文档下可适当提高到 20000–24000，减少批次数（已在 `wiki_ingest_cite.go:24` 常量化，易调）。
- 对超大文档，content 截断点 `maxContentForWiki`（`wiki_ingest.go:39` = 32768）可在「实体密度高」场景下按需下调，降低单轮 token 成本。

---

## 四、验证状态
- 代码改动已写入上述文件，正通过带 `libsqlite3-dev` 的项目 Docker 镜像重新构建（`docker build -f docker/Dockerfile.app`），随后 `docker compose --profile full up -d app` 部署并复测 `/health` 与图谱抽取。
- 单测覆盖：`extract_entity_test.go` 已含 `GRAPH_EXTRACT_FAILED` 四类解析失败的回归用例；本次新增的 `RepairJSON`/`SalvageJSONToMaps` 可直接被既有 `parseOutput` 用例复用。
