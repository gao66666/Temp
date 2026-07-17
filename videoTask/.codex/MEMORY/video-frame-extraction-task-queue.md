# 视频抽帧任务队列方案

## 1. 目标与范围

用户上传视频后，系统创建视频处理任务。多个 Worker 通过 Redis Streams Consumer Group 竞争领取任务，使用 FFmpeg 抽帧，保存处理结果，然后确保以下两个收尾动作最终完成：

1. 原视频任务消息完成 Redis `XACK`。
2. Java 后端收到一次业务有效的处理结果回调。

系统需要支持 Worker 崩溃恢复、长任务动态超时、失败退避重试、不可恢复错误进入 DLQ、横向扩展、优雅退出和临时文件清理。

## 2. 总体架构

```text
Producer / Java
    ↓ XADD
Redis Stream: video:jobs
    ↓ XREADGROUP
Consumer Group
    ├── Worker 1
    ├── Worker 2
    └── Worker 3
          ↓
       FFmpeg 抽帧
          ↓
       持久化抽帧结果
          ↓
       创建任务收尾记录
          ↓
       Worker 恢复空闲

任务收尾程序
    ├── 保证原视频消息最终 XACK
    └── 保证 Java 回调最终成功
          ↓
       两项均完成后结束收尾
```

这里的“任务收尾记录”不是第二次 Java 通知。写入收尾记录只是登记两个待完成动作；真正的 Java HTTP 回调只有一个。

## 3. Redis Stream 与 PEL

Worker 必须使用 `XREADGROUP` 从视频 Stream 领取任务。

任务被 Consumer Group 投递后：

- 原始消息仍保留在 Stream 中。
- Consumer Group 的 PEL（Pending Entries List）增加该消息的待确认记录。
- PEL 记录消息 ID、当前 Consumer、闲置时间和投递次数等信息。

处理成功后执行：

```text
XACK <stream_key> <consumer_group> <stream_message_id>
```

`XACK` 只把消息从该 Consumer Group 的 PEL 中移除，不等同于删除 Stream 原始消息。原始数据由后续的 `XDEL` 或 `XTRIM` 策略处理。

Worker 崩溃且没有 `XACK` 时，消息继续留在 PEL。恢复程序通过 `XPENDING` 观察消息，并通过 `XCLAIM` 或 `XAUTOCLAIM` 转移超时任务的所有权。Redis Streams 不会自动把失败消息重新 `XADD` 到主队列；重新领取或重新入队必须由应用实现。

## 4. 业务任务状态

当前讨论形成的推荐状态如下：

```text
PREPARED
    ↓ Worker 领取
PROCESSING
    ├── FFmpeg 成功并持久化结果 → MEDIA_READY
    │                                ↓ 创建任务收尾记录
    │                             FINALIZING
    │                                ↓ XACK 与 Java 回调均完成
    │                             COMPLETED
    ├── 可恢复失败 → RETRY_WAIT → PREPARED
    ├── Worker 失联或 FFmpeg 卡住 → RECOVERING → PREPARED
    └── 不可恢复错误或超过最大重试次数 → FAILED / DLQ
```

状态含义：

| 状态 | 含义 |
|---|---|
| `PREPARED` | 任务已经创建，等待 Worker 领取 |
| `PROCESSING` | Worker 正在执行 FFmpeg |
| `RECOVERING` | 当前执行发生异常，正在隔离旧执行并准备恢复 |
| `RETRY_WAIT` | 可恢复失败，等待到达下一次重试时间 |
| `MEDIA_READY` | FFmpeg 已成功，抽帧结果已经可靠保存 |
| `FINALIZING` | 正在补偿 Redis XACK 和 Java 回调 |
| `COMPLETED` | XACK 与 Java 回调都已完成 |
| `FAILED/DLQ` | 不可自动恢复，等待人工处理或重放 |

用户草案中的 `proceed` 对应这里的 `PROCESSING`；`prepareAck` 表达的业务含义更接近 `MEDIA_READY` 或 `FINALIZING`。最终代码采用哪组名称仍可在实现前确认。

关键不变量：任务一旦进入 `MEDIA_READY`，后续恢复流程不得再次执行 FFmpeg，只能补建或继续处理收尾记录。

## 5. 任务收尾记录

FFmpeg 完成并保存结果后，Worker 创建一条持久化收尾记录。记录至少需要包含：

```text
task_id
source_stream_key
source_consumer_group
source_stream_message_id
task_attempt
result_manifest
ack_done
callback_done
callback_event_id
callback_attempt
next_retry_at
last_error
finalizer_owner
finalizer_lease_until
```

其中两个核心字段为：

```text
ack_done       # 原视频消息是否已经完成 XACK
callback_done  # Java 回调是否已经确认成功
```

原草案中的 `isXack`、`isRecall` 分别对应 `ack_done`、`callback_done`。推荐使用后者，避免把 callback 误解成 recall/召回。

收尾程序的收敛逻辑：

| `ack_done` | `callback_done` | 应执行动作 |
|---|---|---|
| `false` | `false` | 尝试 XACK，并尝试 Java 回调 |
| `true` | `false` | 只重试 Java 回调 |
| `false` | `true` | 只重试 XACK |
| `true` | `true` | 将业务任务标记为 `COMPLETED`，XDEL 原 Stream 消息，移出待收尾集合并删除收尾记录 |

两个动作相互独立。XACK 成功但回调失败时，不重新执行 FFmpeg；回调成功但 XACK 失败时，也不重复执行业务回调，只继续完成 XACK。

## 6. 正常处理流程

1. Producer 创建业务任务，初始状态为 `PREPARED`，并向 `video:jobs` 写入消息。
2. Worker 使用 `XREADGROUP` 领取任务，消息进入 PEL。
3. Worker 将任务状态更新为 `PROCESSING`；Task 自己增加并保存 `attempt`，同时绑定本次 `worker_instance_id` 和到期时间。Worker 只在运行期间携带当前 `attempt` 作为提交凭证，不是重试次数的事实来源。
4. Worker 执行 FFmpeg，上传抽帧文件并保存结果 manifest。
5. 结果可靠保存后，将任务更新为 `MEDIA_READY`。
6. Worker 创建收尾记录，初始为 `ack_done=false`、`callback_done=false`。
7. 收尾记录可靠保存后，Worker 可恢复为空闲状态并领取下一个视频任务。
8. 收尾程序独立完成原视频消息的 XACK 和 Java 回调。
9. 两项均完成后，将任务标记为 `COMPLETED`，执行 `XDEL` 删除原 Stream 消息，并从待收尾集合移除、删除收尾记录。

不能在收尾记录可靠保存之前让 Worker 结束任务，否则进程可能在中间崩溃，导致系统既没有完成 XACK，也没有留下 Java 回调的补偿依据。

## 7. 收尾队列的存储建议

如果使用 Redis Stream 保存收尾任务，需要注意 Stream 消息字段写入后不能原地修改，因此不能直接修改消息内的 `ack_done` 和 `callback_done`。

当前推荐组合：

```text
ZSET completion:pending
    - member: task_id
    - score: next_retry_at

HASH completion:task:<task_id>
    - 保存收尾记录和两个完成标记
```

ZSET 用于按下次执行时间获取到期任务，避免定时任务每次全量扫描所有记录。Hash 或业务数据库保存可变状态。

当前已经确定：Java 后端的业务数据库是“任务是否最终完成”的事实来源；Redis 中的 Stream、PEL、运行状态、重试 ZSET 和收尾记录属于可重建的调度数据。

业务数据库不要求保存 Redis 的全部中间状态。最小业务状态为：

```text
PENDING
SUCCEEDED
FAILED
CANCELLED
```

`ack_done` 与 `callback_done` 仍可保存在 Redis 收尾记录中。如果 Redis 丢失这些记录，后端会发现对应业务 Task 长时间仍为 `PENDING`，使用相同 `task_id` 重新投递。该恢复方式可能导致 FFmpeg 和回调重复执行，但当前方案接受重复执行，并通过幂等避免重复业务副作用。

## 8. XACK 补偿

收尾程序执行 XACK 时需要使用收尾记录保存的：

```text
source_stream_key
source_consumer_group
source_stream_message_id
```

由于 XACK 和 Redis Hash 状态更新都发生在 Redis，可考虑使用 Lua 原子完成：

```text
XACK 原视频消息
HSET completion:task:<task_id> ack_done 1
```

这样可以避免 XACK 成功后、`ack_done` 更新前进程崩溃。

如果 XACK 返回 `0`，可能表示消息已经被确认、消息 ID/消费组不正确或消息已不在对应 PEL。实现时不能无限盲重试，需要结合保存的消息信息和 PEL 状态进行核对并产生告警。

`XACK` 只移除 PEL 记录，不删除 Stream 消息。当前确定的清理顺序为：后端终态回调已经提交、`callback_done=true`，且原消息已经 `XACK`、`ack_done=true` 后，Scheduler 才执行 `XDEL`。`XDEL` 与任务完成标记、待收尾 ZSET 移除、收尾 Hash 删除放在同一个 Redis transaction 中。重复执行时 `XDEL` 返回 `0` 视为消息已经不存在，可以继续收敛；DLQ 消息采用独立保留策略，不随原消息删除。

## 9. Java 回调补偿与幂等

Java HTTP 回调与 Redis 状态更新无法组成同一个原子事务，始终存在以下窗口：

```text
Java 已经处理成功
    ↓
收尾程序在写 callback_done=true 前崩溃
    ↓
下次再次发送回调
```

因此回调必须携带稳定的幂等标识，例如：

```text
event_id = <task_id>:media-ready:<result_version>
```

Java 端记录已经处理过的 `event_id`。重复收到相同事件时，不重复执行业务副作用，直接返回成功。

回调成功后设置：

```text
callback_done = true
```

回调失败则保持 `callback_done=false`，记录错误和重试次数，并根据退避策略更新 `next_retry_at`。回调重试不得将任务状态退回 `PROCESSING`，也不得重新运行 FFmpeg。

## 10. 心跳、到期恢复与收尾检查

当前方案明确区分四套机制，不能把它们合并为同一个定时扫描：

| 机制 | 检查对象 | 目的 |
|---|---|---|
| Worker 心跳监测 | Worker 进程实例 | 判断 Worker 是否失联 |
| Task 到期检查 | 已被领取且正在执行的 Task | 判断执行是否进入异常状态，是否需要恢复并重新进入任务池 |
| 延迟重试调度 | `RETRY_WAIT` Task | 到达 `next_retry_at` 后重新进入视频任务池 |
| 通知/收尾队列定期检查 | FFmpeg 已完成的收尾记录 | 分别补偿原视频消息 XACK 和 Java 回调 |

通知/收尾队列定期检查只处理 `ack_done` 与 `callback_done`，不负责判断 FFmpeg 是否超时，也不把已经 `MEDIA_READY` 的任务重新交给 FFmpeg。

### 10.1 attempt 由 Task 维护

`attempt` 属于 Task，不属于 Worker。Task 至少保存：

```text
task_id
status
attempt
owner_worker_instance_id
stream_message_id
deadline_at
last_progress_at
next_retry_at
```

Worker 领取任务时，Task 增加 `attempt` 并绑定本次 `worker_instance_id`。Worker 在本地携带 `(task_id, attempt, worker_instance_id)`，所有心跳、进度和完成结果都必须使用这组凭证做条件更新。

例如当前 Task 已经进入 attempt 3，旧 Worker 持有的 attempt 2 不允许再更新心跳、覆盖状态或提交结果。这一校验用于隔离失联后恢复的旧 Worker。

### 10.2 Worker 心跳

每次 Worker 进程启动时生成唯一的 `worker_instance_id`，并周期性发送心跳：

```text
worker_instance_id
heartbeat_at
```

一段宽限时间内没有收到心跳，表示该 Worker 实例已经不可用。原因可能是进程崩溃、OOM、容器重启、节点故障或网络中断；调度系统不需要区分具体原因即可启动失联恢复。

Worker 心跳只回答“Worker 是否仍然可用”。Task 的 `attempt`、到期时间和重试次数仍由 Task 自己维护。

如果失联 Worker 没有持有任务，只需要将该实例视为离线并交给进程管理器重启。如果它持有 `PROCESSING` Task，则将 Task 交给到期/异常恢复流程。不能仅在 Redis 中把旧 Worker 状态改成空闲；新启动的进程必须使用新的 `worker_instance_id`。

### 10.3 Task 到期检查

Task 到期检查用于发现正在执行的 Task 是否异常，不用于处理通知队列，也不负责调度 `RETRY_WAIT`。可使用按 `deadline_at` 排序的 ZSET，只获取已经到期的 `PROCESSING` Task，避免全量扫描。

到期后先读取 Task 当前状态：

- `PROCESSING`：检查所属 Worker 心跳和 FFmpeg 进度，判断是 Worker 失联、FFmpeg 卡住还是视频只是处理较慢。
- `MEDIA_READY`：表示状态已在扫描期间前进；不得重新执行 FFmpeg，清理处理到期索引并确认收尾记录存在。
- `FINALIZING` 或 `COMPLETED`：清理残留的处理到期索引，不执行视频重试。
- 其他状态：说明到期索引已经过时，按当前状态对应的专用机制处理，不能仅因索引到期而重置任务。

`PROCESSING` Task 的判断关系：

| Worker 心跳 | FFmpeg 进度 | 处理方式 |
|---|---|---|
| 正常 | 持续增长 | 视频处理较慢，允许延长 `deadline_at` |
| 正常 | 长时间不增长 | FFmpeg 可能卡住，隔离当前 attempt、终止进程组并重试 |
| 消失 | 无法确认 | Worker 失联，隔离当前 attempt，并通过 PEL 恢复原任务 |

确认需要恢复时：

1. 将 Task 标记为 `RECOVERING`，使旧 attempt 失效。
2. 终止或确认原 FFmpeg 进程组/Worker 实例已经停止；即使无法立即终止，attempt 校验也必须阻止旧实例提交结果。
3. Worker 突然失联时，原消息仍在 PEL，通过 `XCLAIM` 转移原消息，而不是无条件创建重复消息。
4. 明确的可恢复失败可进入 `RETRY_WAIT`，到期后重新进入视频任务池。
5. 超过最大重试次数或遇到不可恢复错误时进入 DLQ。

仅使用 PEL idle time 或预计处理时间不能证明 FFmpeg 已经死亡。`XPENDING` 的 idle time 是消息距离上次投递或 claim 的时间，不是 Worker 心跳，也不是 FFmpeg 活跃进度。

### 10.4 FFmpeg 进度与卡死判断

FFmpeg 提供机器可读的进度输出，Worker 启动 FFmpeg 时使用：

```text
-progress pipe:1
-stats_period <间隔秒数>
-nostats
```

FFmpeg 会周期性输出 `key=value`，每组以 `progress=continue` 或 `progress=end` 结束。Worker 解析 `frame`、`out_time_us`、`out_time_ms` 等字段。

只有在帧数或媒体处理时间实际增长时，才更新 Task 的 `last_progress_at`。仅收到重复的 `progress=continue` 不能说明 FFmpeg 正在前进。

FFmpeg 本身不会直接报告“已经卡死”，到期检查根据以下组合判断：

```text
Worker 心跳仍然正常
Task 仍为 PROCESSING
frame/out_time 长时间没有增长
```

满足条件后，将当前 attempt 标记为失效，先向 FFmpeg 进程组发送 `SIGTERM`，宽限期后仍未退出则发送 `SIGKILL`，随后进入重试或 DLQ。

建议分别设置：

```text
progress_timeout  # 允许 FFmpeg 多久没有实际进度
hard_deadline     # 单次执行允许的绝对最长时间
```

具体心跳周期、宽限时间、进度超时和绝对上限仍需结合视频规模和部署环境确定。

## 11. 失败重试与 DLQ

可恢复的 FFmpeg 失败进入延迟重试集合，采用指数或分级退避，例如：

```text
1 分钟 → 4 分钟 → 9 分钟
```

定时调度器只扫描已经到期的任务，再将它们交回主处理流程。重试转移需要避免“从延迟集合移除后、写回主队列前崩溃”造成任务丢失，可使用 Redis Lua 或其他原子转移机制。

以下不可恢复错误直接进入 DLQ：

- 文件损坏且无法解码。
- 明确不支持的格式或编码。
- 输入对象不存在且确认不会恢复。
- 超过明确的安全或业务限制。

DLQ 记录应包含任务 ID、输入信息、错误类型、错误摘要、处理次数、首次/最后失败时间和人工重放所需数据。

## 12. Redis 故障与业务数据库兜底

Redis 持久化、高可用和备份用于缩小数据丢失窗口，但不能作为“所有写入绝对不丢”的保证。当前方案接受 Redis 中最近的 Stream 消息、PEL 状态、重试 ZSET、运行状态或收尾记录发生丢失；业务数据库负责发现未完成任务并重新投递。

### 12.1 Producer 先写数据库再投递

Producer 创建任务时必须先提交业务数据库：

```text
INSERT Task
status = PENDING
callback_received = false
```

数据库提交成功后，再执行：

```text
XADD video:jobs ... task_id=<稳定业务ID>
```

如果数据库成功而 `XADD` 失败，Task 仍然存在，后端补偿程序可以重新投递。如果 `XADD` 成功后 Producer 崩溃，补偿程序可能再次投递相同 `task_id`；当前方案接受重复 Redis 消息，Worker 和输出处理必须保持幂等。

业务数据库建议至少保存：

```text
task_id
status
created_at
last_dispatched_at
dispatch_count
callback_received
completed_at
result_manifest
last_error
```

多个 Producer 或补偿实例并发重新投递时，应使用数据库条件更新、行锁或 Outbox，保证同一轮只有一个实例获得投递权。

### 12.2 后端以终态回调确认完成

Java 后端收到成功或最终失败回调后，在数据库事务中更新 Task：

```text
SUCCEEDED  # 抽帧和结果保存成功
FAILED     # 不可恢复错误或超过最大重试次数
```

事务提交后才能向回调方返回成功。回调使用稳定的 `event_id` 幂等；如果 Java 已提交事务但 HTTP 响应丢失，再次收到同一事件时不得重复执行业务副作用，应直接返回成功。

最终失败也必须回调，否则数据库中的 Task 会永久保持 `PENDING`，Producer 补偿程序会不断重新投递不可恢复任务。

### 12.3 未完成任务的重新确认与投递

后端定期查找长时间没有收到终态回调的 `PENDING` Task。不能看到 `PENDING` 就立即投递，因为原 FFmpeg 可能仍在正常运行；需要结合：

```text
last_dispatched_at
任务最大预期处理时间
额外宽限时间
dispatch_count
```

超过重新确认阈值后，使用同一个 `task_id` 再次写入视频任务 Stream。该机制保证 Redis Stream 消息完全丢失时，业务任务仍能最终重新出现。

数据库恢复只保证“最终会再次执行”，不保证“只执行一次”。因此必须满足：

- 数据库 `task_id` 全局唯一且稳定。
- Redis 重投始终沿用原 `task_id`。
- 抽帧输出目录和对象存储 key 由 `task_id + result_version` 确定，重复上传可覆盖或安全复用。
- Java 回调使用稳定 `event_id` 幂等。
- 已进入 `SUCCEEDED`、`FAILED` 或 `CANCELLED` 的业务 Task 不再重新投递。
- 重复处理不得产生重复计费、重复通知等业务副作用。

### 12.4 Redis 暂时不可用

Redis 暂时不可用时：

1. Producer 保留数据库中的 `PENDING` Task，等待补偿投递。
2. Worker 停止领取新任务，不把 Redis 连接错误当作 FFmpeg 业务失败。
3. 已经执行的 FFmpeg 可以完成对象存储写入；如果无法创建收尾记录，后端最终会因未收到回调而重新投递，允许产生重复处理。
4. Redis 恢复后先进入心跳恢复宽限期，等待仍存活的 Worker 使用原 `worker_instance_id` 重新发送心跳，再判定失联实例，避免把 Redis 故障造成的统一心跳中断误判为所有 Worker 同时崩溃。

### 12.5 Redis 数据丢失后的重建

当前采用最小业务状态设计，Redis 整体丢失后不要求精确恢复每个 PEL、attempt 或收尾标记。恢复程序以数据库为准：

| 数据库状态 | 恢复动作 |
|---|---|
| `PENDING` 且超过重新确认阈值 | 使用相同 `task_id` 重新投递，接受重复 FFmpeg |
| `SUCCEEDED` | 不重新投递 |
| `FAILED` | 不自动投递；需要人工重放时创建新的执行版本 |
| `CANCELLED` | 不重新投递 |

如果后续需要降低重复 FFmpeg 成本，可以再把 `MEDIA_READY`、`result_manifest` 等中间结果持久化到数据库，从而在 Redis 丢失后直接补回调而不重新抽帧；这不是当前第一版的必要条件。

### 12.6 Redis 部署基线

建议生产环境至少采用：

```text
AOF appendfsync everysec
定期 RDB
持久化磁盘
Primary + Replica
Sentinel 自动故障转移（不分片场景）
maxmemory-policy noeviction
定期备份和恢复演练
```

该配置用于提高可用性和减少数据丢失，不替代数据库兜底。Redis 主从默认异步复制，即使客户端收到写入成功，故障切换仍可能丢失尚未复制的最近写入。关键写入可以评估 `WAIT` 或 `WAITAOF`，但系统仍按 Redis 可能丢数据设计。

## 13. FFmpeg 进程与优雅退出

- FFmpeg 应运行在独立进程组中。
- 终止时先发送 `SIGTERM`，宽限期后再发送 `SIGKILL`。
- 必须调用 `wait()` 回收子进程，避免僵尸进程。
- Worker 收到退出信号后停止领取新任务。
- 当前任务在退出宽限期内可继续完成；未完成则不得错误地标记 `MEDIA_READY` 或创建成功收尾记录。
- 未完成的视频消息保留在 PEL，等待 Task 到期后的异常恢复流程。

## 14. 临时文件清理

每个任务使用独立工作目录：

```text
/tmp/video-worker/<task_id>/<attempt>/
```

正常完成或失败时在 `finally` 中清理。启动时和定时任务可清理超过安全时间、且没有有效任务租约的孤儿目录。不能仅按目录时间无条件删除整个 `/tmp`，否则可能误删仍在处理的文件。

## 15. 当前已确认的设计原则

1. 使用 Redis Streams + Consumer Group 分发视频任务。
2. 通过 PEL 保存未确认消息，并由恢复程序处理失联任务。
3. FFmpeg 结果可靠保存后创建持久化收尾记录。
4. 收尾记录使用两个独立完成标记：`ack_done` 与 `callback_done`。
5. 两个标记都完成后，任务才能结束收尾并移出待处理集合。
6. XACK 只是 Redis Consumer Group 的内部确认，不是 Java 业务通知。
7. 正常业务路径只调用一次 Java；异常重试可能重复发送，因此 Java 必须幂等。
8. Worker 在收尾记录可靠创建后即可恢复空闲，不需要等待 Java 回调完成。
9. 任务进入 `MEDIA_READY` 后，不得因 XACK 或回调失败重新运行 FFmpeg。
10. `attempt` 由 Task 持久化维护；Worker 只携带当前 attempt 作为条件更新凭证。
11. Worker 心跳、Task 到期恢复、延迟重试调度、通知/收尾队列定期检查是四套独立机制。
12. Worker 心跳消失表示该实例不可用；Worker 持有的 Task 由异常恢复流程接管。
13. FFmpeg 卡住归 Task 到期检查处理，通过 `-progress` 输出判断实际进度是否持续增长。
14. Task 到期后必须先检查当前状态；只有异常的处理阶段任务才重新进入任务池，`MEDIA_READY` 之后不得重跑 FFmpeg。
15. Java 后端业务数据库是任务最终状态的事实来源；Producer 必须先写数据库，再投递 Redis。
16. Redis 数据允许丢失，后端通过长时间未收到终态回调的 `PENDING` Task 重新投递相同 `task_id`。
17. 当前方案接受 Redis 消息、FFmpeg 和回调重复执行，但数据库更新、输出路径和 Java 回调必须幂等。
18. Java 后端只有在成功提交 `SUCCEEDED` 或 `FAILED` 事务后才能确认回调成功。

## 16. 实现前仍需讨论的事项

1. 第一版模拟后端使用 SQLite，正式 Java 后端使用哪种数据库，以及最终 Outbox/条件重投的实现方式。
2. Redis 运行状态和收尾记录采用 Hash + ZSET 的具体 key 结构、Lua 原子边界和清理策略。
3. 收尾程序采用固定周期扫描，还是 ZSET 到期调度加事件触发。
4. 业务状态最终沿用 `prepared/proceed/prepareAck`，还是改为语义更明确的 `PREPARED/PROCESSING/MEDIA_READY/FINALIZING/COMPLETED`。
5. Worker 心跳周期、失联宽限时间、FFmpeg `progress_timeout`、单次执行 `hard_deadline` 和最大重试次数。
6. Java 回调协议、成功判定、幂等键保存方式和回调 DLQ 策略。
7. 抽帧规则：固定间隔、固定数量、关键帧或场景检测。
8. 原视频与抽帧结果的对象存储/CDN 地址和生命周期策略。
9. Redis 正式部署采用 Sentinel 还是托管 Redis，以及 AOF、备份、`WAIT/WAITAOF` 的耐久性目标。

## 17. Python 第一版实现计划

### 17.1 第一版目标

先实现一个可在单机运行、能够验证主流程和故障恢复的最小系统：

```text
模拟 Java 后端（FastAPI + SQLite）
    ↓ 创建数据库 Task 后 XADD
Redis（Docker，开启 AOF）
    ↓ XREADGROUP
Python Worker
    ↓ FFmpeg 抽帧和进度解析
本地输出目录 + manifest
    ↓ 创建 Redis 收尾记录
Python Scheduler
    ├── Worker 心跳与 Task 到期恢复
    ├── 延迟重试
    ├── XACK 与回调收尾
    └── 数据库 PENDING Task 重新投递
模拟 Java 回调接口
    ↓
SQLite Task 更新为 SUCCEEDED / FAILED
```

第一版使用本地文件模拟 CDN/对象存储，不在初期引入真实云存储 SDK。所有抽帧输出使用确定性目录，便于验证重复执行和幂等。

### 17.2 本机环境基线

当前检查结果：

- Conda 可用，base 为 Python 3.13.9。
- 已有 `crawlVideo` 环境为 Python 3.11.15，但不应默认污染其他项目环境。
- Docker 可用。
- SQLite CLI 可用。
- `redis-server`、`redis-cli` 和 `ffmpeg` 当前不在 PATH。

推荐创建独立 Conda 环境，例如 `videoTask`，使用 Python 3.11，并通过 Conda 安装 FFmpeg。Redis 第一版通过 Docker Compose 启动，避免修改系统 Redis。

计划依赖：

```text
运行依赖：
fastapi
uvicorn
pydantic-settings
sqlalchemy 2.x
redis-py
httpx

测试依赖：
pytest
pytest-cov
```

依赖版本在环境实际创建并验证兼容后锁定到项目配置中，不直接复用 base 或 `crawlVideo` 的未锁定包。

### 17.3 计划目录结构

```text
videoTask/
├── pyproject.toml
├── environment.yml
├── compose.yaml
├── .env.example
├── src/video_task/
│   ├── config.py
│   ├── domain/
│   │   ├── enums.py
│   │   └── schemas.py
│   ├── backend/
│   │   ├── api.py
│   │   ├── models.py
│   │   ├── repository.py
│   │   └── routes.py
│   ├── redis_store/
│   │   ├── client.py
│   │   ├── keys.py
│   │   └── scripts.py
│   ├── services/
│   │   ├── producer.py
│   │   ├── ffmpeg_runner.py
│   │   ├── finalizer.py
│   │   ├── retry.py
│   │   └── recovery.py
│   └── runners/
│       ├── api.py
│       ├── worker.py
│       └── scheduler.py
├── tests/
│   ├── unit/
│   └── integration/
└── var/
    ├── db/
    ├── input/
    ├── output/
    └── tmp/
```

`var/` 是运行时目录，后续需要加入 Git 忽略。第一版即使将多个调度循环放在同一个 Scheduler 进程，也必须保持类和状态边界独立，不能把 Worker 心跳、Task 到期、延迟重试和通知收尾写成一个混合判断函数。

### 17.4 模拟后端接口

计划暴露：

```text
POST /api/v1/tasks
    创建数据库 Task，提交后向 Redis 投递

GET /api/v1/tasks/{task_id}
    查询最终业务状态、投递次数和结果信息

POST /api/v1/tasks/{task_id}/callback
    模拟 Java 回调；按 event_id 幂等更新 SUCCEEDED / FAILED

POST /api/v1/tasks/{task_id}/redispatch
    开发/测试用手动重新投递接口

GET /health/live
GET /health/ready
```

任务创建请求第一版计划接受：

```text
source_uri
interval_seconds
result_version
```

默认抽帧策略建议为固定时间间隔，具体默认值实现前确认。

### 17.5 SQLite 数据模型

`tasks` 表至少包含：

```text
task_id                 唯一业务 ID
status                  PENDING / SUCCEEDED / FAILED / CANCELLED
source_uri
interval_seconds
result_version
created_at
last_dispatched_at
dispatch_count
callback_received
callback_event_id       唯一或条件唯一
completed_at
result_manifest
last_error
```

回调接口在同一数据库事务中完成幂等检查和终态更新，提交后才返回 2xx。

### 17.6 Redis key 与数据结构

第一版计划使用：

```text
video:jobs                         Stream，主任务队列
video:workers                      Consumer Group 名称
video:dlq                          Stream，最终失败记录

worker:heartbeat:<instance_id>     String/Hash + TTL

task:runtime:<task_id>             Hash，Redis 运行状态
task:deadlines                     ZSET，PROCESSING 到期索引
task:retry                         ZSET，RETRY_WAIT 到期索引

task:finalize:<task_id>            Hash，ack_done/callback_done
task:finalize:pending               ZSET，待收尾索引
```

Redis 运行状态中的 `attempt` 属于 Task。Worker 进程只携带领取时获得的 attempt，所有状态和结果更新必须校验 `task_id + attempt + worker_instance_id`。

### 17.7 Worker 主流程

1. 启动时创建唯一 `worker_instance_id`，注册 Consumer，并开始发送带 TTL 的心跳。
2. 使用 `XREADGROUP ... > BLOCK` 领取新任务。
3. Task 自增 `attempt`，写入 `PROCESSING` 运行状态和 `deadline_at`。
4. 获取输入视频，第一版本地路径优先；HTTP/CDN 下载能力作为可配置适配器。
5. 使用 FFmpeg `-progress pipe:1` 执行抽帧，同时消费 stdout/stderr。
6. 只有 `frame` 或 `out_time` 实际增长时更新 `last_progress_at`。
7. 使用确定性目录输出帧文件并生成 manifest。
8. FFmpeg 成功后将 Redis Task 更新为 `MEDIA_READY`，创建 `ack_done=false/callback_done=false` 的收尾记录。
9. 收尾记录可靠创建后，Worker 返回领取循环，不直接等待 Java 回调。
10. 明确可恢复失败进入 `RETRY_WAIT`；不可恢复错误或超过最大次数进入 DLQ，并创建最终失败回调收尾记录。

### 17.8 Scheduler 逻辑边界

第一版可以在一个 Scheduler 进程内运行四个独立循环：

1. **Task 到期恢复**：只检查过期的 `PROCESSING` Task，结合 Worker 心跳和 FFmpeg 进度判断失联、卡住或允许延长。
2. **延迟重试调度**：只处理到达 `next_retry_at` 的 `RETRY_WAIT` Task，重新进入视频 Stream。
3. **通知/收尾处理**：分别补偿 `XACK` 和模拟 Java 回调；两个标记都完成后清理收尾记录。
4. **数据库任务重新确认**：扫描超过确认阈值且仍为 `PENDING` 的 SQLite Task，条件更新获得投递权后重新 XADD。

多个 Scheduler 并发时需要收尾租约或原子领取；第一版先支持单 Scheduler，并为后续多实例保留 `owner/lease_until` 字段。

### 17.9 原子性与幂等

第一版优先实现以下边界：

- Producer：数据库先提交，Redis 投递失败由数据库扫描补偿。
- XACK：使用 Lua 将原视频消息 `XACK` 与 `ack_done=true` 合并。
- 延迟重试：使用 Lua 保证 ZSET 与 Stream 之间的转移不会丢任务。
- Java 回调：使用 `event_id` 和 SQLite 唯一约束幂等。
- 输出文件：使用 `task_id/result_version` 确定路径，重复执行不产生无界副本。
- 旧 Worker：使用 Task attempt 条件更新，拒绝失效实例的结果。

### 17.10 分阶段实施

#### 阶段 0：环境与骨架

- 创建独立 Conda 环境。
- 安装 FFmpeg 和 Python 依赖。
- 添加 Docker Compose Redis，开启 AOF 和持久卷。
- 创建配置、日志、目录骨架和健康检查。

验收：API、Redis、SQLite 均可启动，FFmpeg 版本检查通过。

#### 阶段 1：模拟后端与 Producer

- 实现 SQLite Task 模型和迁移/初始化。
- 实现创建、查询、回调和手动重投接口。
- 实现“先数据库、后 XADD”和 PENDING 补偿扫描。

验收：Redis 停止时仍能创建 PENDING Task；恢复后可重新投递。

#### 阶段 2：Worker 正常路径

- Consumer Group 创建和阻塞领取。
- Task attempt、Worker 心跳和运行状态。
- FFmpeg 抽帧、进度解析、确定性输出和 manifest。
- 成功后创建收尾记录。

验收：样例视频最终产生帧文件和 manifest，Worker 可以继续领取下一任务。

#### 阶段 3：收尾闭环

- 实现 `ack_done/callback_done` 收尾扫描。
- 实现 XACK Lua 和回调幂等。
- 两项完成后更新 Redis 状态并清理待收尾记录。

验收：正常路径 Java 模拟后端只产生一次业务终态；人为制造回调超时后只重试回调，不重跑 FFmpeg。

#### 阶段 4：异常恢复

- 实现 FFmpeg progress timeout 与 hard deadline。
- 实现 Worker 失联检测、旧 attempt 隔离和 PEL `XCLAIM`。
- 实现延迟重试和 DLQ。

验收：杀死 Worker、卡住 FFmpeg、制造临时失败时，Task 均能恢复或进入 DLQ。

#### 阶段 5：Redis 故障演练

- Redis 普通重启，验证 AOF/PEL 恢复。
- 模拟 Redis 数据清空，验证 SQLite `PENDING` Task 重新投递。
- 验证重复 FFmpeg、重复 XADD 和重复回调不会造成错误终态或无界输出。

验收：Redis 数据丢失后，未完成数据库 Task 最终仍进入 `SUCCEEDED` 或 `FAILED`。

### 17.11 测试清单

单元测试：

- Task 状态转换和 attempt 条件更新。
- FFmpeg `key=value` 进度解析。
- 退避时间计算。
- callback event 幂等。
- 确定性输出路径。

集成测试：

- 正常抽帧和回调。
- 回调失败后重试但不重跑 FFmpeg。
- XACK 失败后的补偿。
- Worker 在 FFmpeg 处理中被强制终止。
- FFmpeg 长时间不更新进度。
- Redis 重启和数据清空。
- Producer 数据库成功但 Redis 投递失败。
- 相同 `task_id` 重复 XADD。
- 最终失败回调和 DLQ。

测试视频由 FFmpeg `lavfi` 在测试准备阶段生成，避免依赖外部 CDN 或版权素材。

## 18. Python 原型实施状态（2026-07-16）

第一版原型已经落地，采用 Python 3.11、FastAPI、SQLAlchemy/SQLite、redis-py、FFmpeg 和 Docker Compose Redis。为避免占用设备上已有的 `6379`，本项目 Redis 暴露在宿主机 `6380`；Redis 开启 AOF `everysec`、持久卷与 `noeviction`。

已经实现：

- 模拟后端的 Task 创建、查询、幂等回调和手动重投接口。
- 数据库先落库、Redis 后投递，以及 Redis 恢复后的 PENDING Task 补偿投递。
- Redis Stream Consumer Group、PEL、Worker 唯一实例标识、心跳和 Task attempt fencing。
- FFmpeg 进程组管理、`-progress pipe:1` 解析、确定性输出目录和 manifest。
- 成功/失败收尾记录，`ack_done` 与 `callback_done` 分别补偿。
- 两个收尾动作完成后删除原 Stream 消息和收尾记录，业务历史由数据库保存；DLQ 独立保留。
- 动态到期检查、Worker 失联恢复、`XCLAIM`、延迟重试与 DLQ。

已完成的验证：

1. 单元测试与 Redis 恢复集成测试共 6 项通过；自动化覆盖率当前为 48%。
2. 12 秒本地样例视频可以完成抽帧、生成 manifest、XACK 和模拟后端回调，最终 PEL 为空。
3. Redis 普通重启后 Consumer Group、运行状态和回调结果可以从 AOF 恢复。
4. Redis 停止期间创建的 Task 会以数据库 `PENDING` 状态保留；Redis 恢复后由 Scheduler 自动重新投递并成功完成。
5. 已完成 Task 被重复 XADD 时，Worker 直接 ACK 重复消息，不会重复覆盖结果。
6. 缺失输入文件会按配置进入延迟重试；超过最大 attempt 后进入 DLQ，并向模拟后端回调 `FAILED`。

当前原型边界：输入源先支持本地路径，尚未实现真实 CDN 下载；后端数据库暂为 SQLite；Scheduler 第一版按单实例运行；部署到容器编排环境后，进程恢复还需要替换为适合 Pod/容器边界的实现。
