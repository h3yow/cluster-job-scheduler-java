# Cluster Job Scheduler（本地批处理作业调度）

在单个 Java 进程内用“节点 + 资源 + 租户配额”模型模拟一个小集群，提供批处理作业的
**提交、排队、按优先级分派、失败重试、取消、状态查询**能力。所有状态变更由调度核心内
一把公平锁串行化，因此在并发提交 / 取消 / 完成回报下不会超卖、不会重复占位、不会出现
状态互相覆盖。

## 1. 模型与状态

- 资源：`Resources(cpuCores, memoryMb)`，两维均为非负整数；节点上的资源必须两维同时满足。
- 节点 `ClusterNode`：有总容量 `capacity`、已占用 `used`、剩余 `available`，并登记运行中的作业集合。
- 作业 `Job`：归属于某个租户（`tenant`），声明所需资源、优先级与最大尝试次数。

作业状态机（`JobState`）：

```
            提交通过                 findFit 成功
 (无) ---------------> QUEUED --------------------> RUNNING
                        ^  |                          | |
                        |  | 取消                      | | 取消
            失败且可重试 |  v                          v v
                     QUEUED (attempt+1)          CANCELLED   (终态)
                        ^                             ^
                        |  资源暂时不足，等待            SUCCEEDED  (终态)
                        |                             FAILED     (终态，重试耗尽)
              超额/超容量/队列满（提交时直接拒绝，不经过队列）: REJECTED (终态)
```

- **终态**：`SUCCEEDED / FAILED / CANCELLED / REJECTED`，一旦进入不可再变更。
- 对终态作业的迟到完成回报、取消请求一律忽略，不会覆盖终态。

## 2. 提交、配额与拒绝

`POST /api/scheduler/jobs` 提交 `JobRequest`，返回 `SubmitResult`。提交时按顺序做以下判定：

1. **参数校验**：`jobId` / `tenant` 非空，资源为正数，`maxAttempts>=1`，否则
   `INVALID_ARGUMENT`。
2. **幂等去重**：同一 `jobId` 重复提交不会产生第二次执行，返回
   `duplicate=true` 以及该作业**首次提交后的当前状态**。
3. **租户配额**：对该租户当前“保活作业”（`QUEUED + RUNNING`，不含终态）统计
   - 作业数 `< quota.maxJobs`
   - 已占用资源 + 新申请资源 `<= quota.maxResources`
   任一不满足返回 `QUOTA_EXCEEDED`，消息中带配额、当前占用与申请量。
4. **物理容量**：申请资源超过**全集群总容量**（任何调度都不可能满足）返回
   `INSUFFICIENT_CAPACITY`。
5. **队列容量**：当前 `QUEUED` 作业数达到 `scheduler.max-queue-capacity` 返回
   `QUEUE_FULL`。

拒绝原因（`RejectReason`）彼此可区分：`INVALID_ARGUMENT / DUPLICATE_SUBMISSION /
QUOTA_EXCEEDED / INSUFFICIENT_CAPACITY / QUEUE_FULL`。

> 说明：**配额与物理容量在提交时直接拒绝**；而“当前各节点暂时没有足够空闲资源、但集群
> 总量装得下”的作业会被**接受并排队**（`QUEUED`），待资源释放后分派，其未调度原因可
> 在作业详情中查到。
>
> 因 `QUOTA_EXCEEDED / INSUFFICIENT_CAPACITY / QUEUE_FULL` 被拒的作业会以
> `REJECTED` 终态登记（不占配额、不进队列、不会执行），可通过作业查询看到拒绝原因与事件；
> 用同一 `jobId` 再次提交命中幂等分支。

## 3. 分派规则

- 队列顺序：**优先级** `HIGH > NORMAL > LOW`；同优先级按提交时间 **FIFO**。
- 节点选择：`FIRST_FIT`，按节点配置顺序选第一个两维资源都满足的节点。
- “资源检查（`findFit`）+ 占位（`tryAllocate`）”与所有状态写操作在同一把锁内完成，
  因此并发下不会出现两个作业占位到同一份资源。
- 单轮扫描会跳过当前放不下的作业继续尝试后续作业，因此高优作业占住大块资源时，资源形状
  不同、能插空的低优作业仍可分派；当轮发生过分派会再跑一轮，直到没有新分派。
- 提交、取消、完成回报之后都会自动触发一轮分派；也可用 `POST /api/scheduler/schedule`
  手动触发。

## 4. 重试与终态语义

- 作业开始执行时 `attempt=1`；执行失败回报 `FAILURE` 后：
  - 若 `attempt < maxAttempts`：先在原节点释放资源，`attempt+1` 重新进入 `QUEUED`，
    随后参与下一轮分派；
  - 若 `attempt == maxAttempts`：释放资源并进入 `FAILED` 终态，`terminalReason`
    形如 `max attempts (N) exhausted; lastError=...`。
- 成功回报 `SUCCESS`：释放资源并进入 `SUCCEEDED`。
- `maxAttempts` 可在单个作业上覆盖（`JobRequest.maxAttempts`），否则使用全局
  `scheduler.default-max-attempts`（默认 3）。
- 作业在执行期间被取消：节点资源立即释放、作业进入 `CANCELLED`，之后到达的完成回报
  因作业已非 `RUNNING` 而被忽略。

## 5. 可解释性 / 状态查询

`GET /api/scheduler/jobs/{id}` 返回作业详情，包含：

- 当前 `state`、是否 `terminal`、`attempt / maxAttempts`、`assignedNodeId`；
- `rejectReason`、`terminalReason`；
- `lastScheduleSkip`：最近一次调度未能分派的具体原因（如
  `no node can fit Resources(cpu=8,memoryMb=16000) right now; clusterAvailable=...`）；
- `events`：完整事件流，事件类型包括 `SUBMITTED / ASSIGNED / SCHEDULE_SKIPPED /
  RETRY_QUEUED / SUCCEEDED / FAILED / CANCELLED / REJECTED`，每条带时间戳与判定依据。

`GET /api/scheduler/nodes` 返回各节点 `capacity / used / available / runningJobIds`，
便于核对资源占用。调度核心在提交、拒绝、分派、跳过分派、取消、回报时均有 INFO 日志，
打印关键输入与判定依据。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/scheduler/jobs` | 提交作业（body 为 `JobRequest`） |
| POST | `/api/scheduler/jobs/{id}/cancel?reason=` | 取消排队/运行中的作业 |
| POST | `/api/scheduler/jobs/{id}/completion?outcome=SUCCESS|FAILURE&detail=` | 执行回报 |
| POST | `/api/scheduler/schedule` | 手动触发一轮分派，返回新分派数量 |
| GET  | `/api/scheduler/jobs/{id}` | 查询单个作业（含事件与未调度原因） |
| GET  | `/api/scheduler/jobs` | 列出全部作业 |
| GET  | `/api/scheduler/nodes` | 查询节点与资源占用 |

`JobRequest` 示例：

```json
{
  "jobId": "job-001",
  "tenant": "team-a",
  "name": "nightly-etl",
  "resources": { "cpuCores": 4, "memoryMb": 8192 },
  "priority": "HIGH",
  "maxAttempts": 3
}
```

## 7. 配置项（`application.properties`，前缀 `scheduler`）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `scheduler.default-max-attempts` | 3 | 全局默认最大尝试次数（含首次执行） |
| `scheduler.max-queue-capacity` | 1000 | 等待队列容量，只统计 `QUEUED` 作业 |
| `scheduler.node-selector` | `FIRST_FIT` | 节点选择策略 |
| `scheduler.cluster.nodes[i].id` | `node-1`,`node-2` | 节点 id；不配节点时使用内置两节点集群 |
| `scheduler.cluster.nodes[i].cpu-cores` | 8 | 节点 CPU 核数 |
| `scheduler.cluster.nodes[i].memory-mb` | 16384 | 节点内存（MB） |
| `scheduler.quotas.<tenant>.max-jobs` | 100（兜底配额） | 该租户并发保活作业数上限 |
| `scheduler.quotas.<tenant>.max-cpu-cores` | 64（兜底配额） | 该租户保活作业 CPU 总量上限 |
| `scheduler.quotas.<tenant>.max-memory-mb` | 131072（兜底配额） | 该租户保活作业内存总量上限 |

未显式配置配额的租户使用内置兜底配额（100 作业 / 64 核 / 128GB）。

## 8. 并发正确性

- 全部状态变更入口（提交 / 取消 / 完成回报 / 分派）都在 `SchedulerCore` 的单把
  `ReentrantLock(fair=true)` 内串行完成；锁内不做阻塞等待。
- 节点占位采用“检查可用 → 占用”成对操作，且整体在锁内，杜绝超卖与重复占位。
- 查询接口返回快照副本，不在锁外遍历内部集合。

## 9. 测试

```bash
mvn test
```

覆盖场景（共 14 个用例）：

- 基础分派与资源记账、优先级插队与未调度原因、重复提交幂等、非法参数与超容量拒绝；
- **配额冲突**：作业数上限、资源上限，终态释放配额后可再次提交；
- **失败重试**：失败后 attempt 递增、资源释放与重新排队（用容量竞争确定性地观察排队态）、
  重试耗尽进入 `FAILED`、终态后迟到回报/取消被忽略；
- **并发**：20 线程并发提交不超卖；30 线程并发配额竞争恰有配额数量的赢家且无重复占位；
  40 作业提交后取消 / 成功 / 失败回报多线程交织，最终校验节点占用与 RUNNING 作业严格一致。

测试日志会打印关键输入（线程数、资源申请、容量/配额）与调度判定依据（分派到哪个节点、
跳过原因、拒绝原因与配额占用明细）。

## 10. 代码结构

```
scheduler/
  model/     Resources、JobState、JobPriority、RejectReason、JobEvent、
             JobRequest、SubmitResult、CompletionOutcome
  domain/    Job（状态机 + 事件流）、ClusterNode（占位/释放）
  cluster/   Cluster（容量聚合、FIRST_FIT）
  quota/     QuotaRecord、QuotaManager
  engine/    SchedulerCore（锁、提交/取消/回报/分派循环、可解释事件）
  config/    SchedulerProperties、SchedulerConfiguration
  web/       SchedulerController（REST）
```
