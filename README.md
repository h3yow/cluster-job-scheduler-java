# Cluster Job Scheduler（本地批处理作业调度器）

一个基于 Spring Boot 的**进程内**批处理作业调度服务：用单机的「节点 + 资源」模型模拟一个小集群，
提供作业提交、排队、按优先级分派、失败重试与状态查询。所有状态变更由**单一锁串行化**，
因此在提交 / 取消 / 失败重试并发发生时，不会出现超卖、重复占位或状态互相覆盖。

## 1. 资源模型

- 资源为两维：`cpu`（核数）与 `memoryMb`（内存 MB），均为正整数（见 `model/Resources`）。
- 节点 `ClusterNode` 维护 `capacity`（总容量）与 `used`（已分配），`canFit` 要求两维同时满足。
- 集群由配置 `scheduler.nodes` 初始化，例如 `node-1:8:16384,node-2:8:16384`。

## 2. 提交与配额规则

`POST /api/jobs`，请求体：

```json
{ "id": "job-1", "tenant": "team-a", "priority": 5, "cpu": 2, "memoryMb": 2048 }
```

- `id` 必填且全局唯一；`tenant` 缺省为 `default`；`priority` 缺省为 0，**数值越大优先级越高**。
- 提交时按顺序做以下校验，拒绝时作业进入终态 `REJECTED` 并登记明确的 `rejectReason`，拒绝作业不占任何资源/配额：

| 拒绝原因 (`RejectReason`)        | 触发条件                                                                 | HTTP |
|----------------------------------|--------------------------------------------------------------------------|------|
| `DUPLICATE_JOB`                  | 同一 `id` 重复提交（并发提交同样保证只有一次被接受，作业**只执行一次**） | 409  |
| `JOB_QUOTA_EXCEEDED`             | 申请的 CPU 或内存超过单作业上限 `scheduler.max-job-*`                    | 422  |
| `EXCEEDS_MAX_CAPACITY`           | 申请超过集群中**任一在线节点**的物理上限（永久放不下，排队无意义）       | 422  |
| `TENANT_QUOTA_EXCEEDED`          | 该租户「排队 PENDING + 运行 RUNNING」的累计占用再加本次申请超过租户配额  | 422  |

- **资源暂时不足 ≠ 拒绝**：申请合法、只是当前没有空闲节点时，作业进入 `PENDING` 排队，
  作业视图的 `blockReason` / `lastDecision` 记录具体原因（例如最宽裕节点的剩余 CPU/内存），
  等任何作业完成释放资源后会立即再触发分派。
- 租户配额把**排队中**的申请也计入，防止「先排队、资源一空出就超配」。配额某维配置为 `0` 表示该维不限。

## 3. 分派（调度）规则

- 每个提交（可由 `scheduler.schedule-on-submit` 关闭）以及每次作业完成/失败后，都会触发一轮调度。
- 待分派作业按 **优先级降序**，同优先级按**提交顺序 FIFO**（全局自增序号）。
- 节点选择采用 **best-fit**：在能放下作业的在线节点中，选择剩余 CPU 最少（其次剩余内存最少）的节点，尽量减少碎片。
- 允许「跳过队首」：高优先级作业暂时放不下时，不会阻塞能放下的低优先级作业。
- **不超卖保证**：节点占位（`allocate`）与状态翻转在调度器的同一把锁内完成；
  工作线程只在锁外执行作业，结束后回到锁内做且仅做一次资源释放与状态流转。
- 每一次分派尝试都会在作业上留下可解释记录：
  - 成功：事件 `DISPATCHED` + `lastDecision.scheduled=true`，含目标节点、分派前后剩余资源、优先级、尝试序号；
  - 受阻：事件 `SCHEDULE_BLOCKED` + `lastDecision.scheduled=false`，含最宽裕节点的可用量。

## 4. 重试与终态语义

作业状态机：

```
PENDING ──分派──> RUNNING ──成功──────────────> SUCCEEDED   （终态）
   ^                │
   │                ├──失败且 attempts < max──> PENDING（退避后重试）
   │                └──失败且 attempts = max──> FAILED      （终态）
   │
   └──取消（排队中）──────────────────────────> CANCELLED   （终态）
RUNNING ──取消请求（协作式，当前尝试结束后生效）> CANCELLED   （终态）

提交阶段拒绝 ─────────────────────────────────> REJECTED    （终态）
```

- 最大尝试次数（**含首次**）由 `scheduler.max-attempts` 控制，达到上限后进入明确终态 `FAILED`，不再重试。
- 失败重试退避：第 `n` 次失败后等待 `scheduler.retry-backoff-ms * n` 毫秒；退避窗口内作业不参与分派。
- 失败后资源**立即释放**并回到 `PENDING`，退避期间集群容量可被其他作业使用。
- 取消运行中的作业是**协作式**的：打上取消标记，等当前一次执行结束后释放资源并置 `CANCELLED`，
  不会在执行中途把资源重复分给别人。取消终态作业无效（接口返回 409）。
- 终态（`SUCCEEDED/FAILED/CANCELLED/REJECTED`）不可逆；终态资源占用恒为 0。

## 5. 查询接口

| 方法 & 路径                | 说明                                                                 |
|----------------------------|----------------------------------------------------------------------|
| `POST /api/jobs`           | 提交作业（202 接受 / 409 重复 / 422 配额或容量拒绝 / 400 参数非法） |
| `GET  /api/jobs`           | 全部作业视图                                                         |
| `GET  /api/jobs/{id}`      | 单个作业：状态、节点、尝试次数、`rejectReason`、`blockReason`、`lastDecision`、完整事件流 |
| `POST /api/jobs/{id}/cancel` | 请求取消（200 / 404 / 409 终态不可取消）                           |
| `GET  /api/nodes`          | 全部节点：容量、已用、可用、在跑作业列表                             |
| `GET  /api/nodes/{id}`     | 单个节点                                                             |

作业从排队到终态的每次流转（`SUBMITTED / REJECTED / DISPATCHED / SCHEDULE_BLOCKED /
ATTEMPT_FAILED / SUCCEEDED / FAILED / CANCEL_REQUESTED / CANCELLED`）都带时间戳与文字依据，
可通过 `GET /api/jobs/{id}` 的 `events` 与 `lastDecision` 复盘。

## 6. 配置项（`src/main/resources/application.properties`，前缀 `scheduler`）

| 配置项                       | 默认值                          | 含义                                           |
|------------------------------|---------------------------------|------------------------------------------------|
| `max-job-cpu`                | `8`                             | 单作业 CPU 上限，超出拒绝 `JOB_QUOTA_EXCEEDED` |
| `max-job-memory-mb`          | `16384`                         | 单作业内存上限                                 |
| `tenant-cpu-quota`           | `16`                            | 单租户 CPU 配额（排队+运行），`0` 不限         |
| `tenant-memory-quota-mb`     | `32768`                         | 单租户内存配额，`0` 不限                       |
| `max-attempts`               | `3`                             | 最大尝试次数（含首次），用尽进入 `FAILED`      |
| `retry-backoff-ms`           | `50`                            | 重试基础退避，实际延迟 = 该值 × 已尝试次数     |
| `schedule-on-submit`         | `true`                          | 提交后是否立即尝试分派                         |
| `nodes`                      | `node-1:8:16384,node-2:8:16384` | 模拟节点 `名称:CPU:内存MB`，逗号分隔           |

## 7. 测试

```bash
mvn test
```

- `SchedulerQuotaTest`：单作业/租户配额、物理容量、重复提交、资源暂时不足排队与释放后分派。
- `ConcurrentSubmitTest`：40 线程并发提交不超卖（巡检节点/集群两级已用量）、同 id 并发提交仅执行一次、
  高优先级先分派、排队取消。
- `RetryTest`：始终失败恰好执行 `maxAttempts` 次后 `FAILED`、失败两次后成功、退避生效、退避期间资源可被复用、终态稳定。
- `SchedulerApiTest`：HTTP 状态码与拒绝原因、作业/节点视图、取消语义。

测试通过可控的 `TrackingHandler`（记录每个作业实际执行次数、检测并发重叠、支持阻塞/失败注入）模拟执行器；
单测日志以 `[input]` / `[decision]` / `[assert]` 前缀打印关键输入、调度判定依据与最终断言。

## 8. 主要代码结构

```
model/        Resources / Job / ClusterNode / JobState / NodeState / RejectReason / JobEvent / SchedulingDecision
config/       SchedulerProperties（@ConfigurationProperties）、SchedulerConfig（初始化节点、生命周期）
scheduler/    JobScheduler（单锁串行化的核心调度器）、JobExecutionHandler、DefaultJobExecutionHandler、SubmitResult
web/          JobController、NodeController 与 dto 视图
```
