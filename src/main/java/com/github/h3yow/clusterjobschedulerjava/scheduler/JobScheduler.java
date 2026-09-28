package com.github.h3yow.clusterjobschedulerjava.scheduler;

import com.github.h3yow.clusterjobschedulerjava.config.SchedulerProperties;
import com.github.h3yow.clusterjobschedulerjava.model.ClusterNode;
import com.github.h3yow.clusterjobschedulerjava.model.Job;
import com.github.h3yow.clusterjobschedulerjava.model.JobState;
import com.github.h3yow.clusterjobschedulerjava.model.NodeState;
import com.github.h3yow.clusterjobschedulerjava.model.RejectReason;
import com.github.h3yow.clusterjobschedulerjava.model.Resources;
import com.github.h3yow.clusterjobschedulerjava.model.SchedulingDecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 进程内批处理作业调度器：用单一锁串行化所有状态变更，
 * 保证提交/取消/失败重试并发发生时不超卖、不重复占位、状态不互相覆盖。
 *
 * <p>资源与配额规则：
 * <ul>
 *   <li>单作业申请超过 scheduler.max-job-* 配置 → 拒绝 JOB_QUOTA_EXCEEDED；</li>
 *   <li>申请超过集群中任一节点的物理上限 → 拒绝 EXCEEDS_MAX_CAPACITY（永久不可调度）；</li>
 *   <li>租户在集群内（PENDING+RUNNING）累计占用超过 scheduler.tenant-* 配额
 *       → 拒绝 TENANT_QUOTA_EXCEEDED；</li>
 *   <li>只是当前没有空闲资源 → 进入队列 PENDING，记录阻塞原因，待资源释放后再分派；</li>
 *   <li>同一 jobId 重复提交 → 拒绝 DUPLICATE_JOB，既有作业不受影响、不会执行多次。</li>
 * </ul>
 */
@Component
public class JobScheduler {

    private static final Logger log = LoggerFactory.getLogger(JobScheduler.class);

    private final SchedulerProperties props;
    private final JobExecutionHandler executionHandler;

    private final Object lock = new Object();
    private final Map<String, Job> jobs = new LinkedHashMap<>();
    private final Map<String, ClusterNode> nodes = new LinkedHashMap<>();
    /** 在锁内按进入终态的先后顺序记录，供查询/测试给出确定性顺序。 */
    private final List<String> terminalOrder = new ArrayList<>();
    private long sequenceGenerator = 0;

    private final ExecutorService workers = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "scheduler-worker");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService retryScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "scheduler-retry");
                t.setDaemon(true);
                return t;
            });

    public JobScheduler(SchedulerProperties props, JobExecutionHandler executionHandler) {
        this.props = props;
        this.executionHandler = executionHandler;
    }

    /** 由配置初始化节点，格式 name:cpu:memoryMb，逗号分隔。 */
    public void initNodes() {
        synchronized (lock) {
            if (!nodes.isEmpty()) {
                return;
            }
            for (String spec : props.getNodes().split(",")) {
                spec = spec.trim();
                if (spec.isEmpty()) {
                    continue;
                }
                String[] parts = spec.split(":");
                if (parts.length != 3) {
                    throw new IllegalStateException("invalid node spec: " + spec);
                }
                String nodeId = parts[0].trim();
                long cpu = Long.parseLong(parts[1].trim());
                long mem = Long.parseLong(parts[2].trim());
                nodes.put(nodeId, new ClusterNode(nodeId, new Resources(cpu, mem)));
                log.info("node initialized nodeId={} capacity=cpu:{},mem:{}", nodeId, cpu, mem);
            }
        }
    }

    /** 提交作业（幂等：同一 id 重复提交只返回既有结果，绝不执行多次）。 */
    public SubmitResult submit(String id, String tenant, int priority, Resources request) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("job id must not be blank");
        }
        if (request == null || request.cpu() <= 0 || request.memoryMb() <= 0) {
            throw new IllegalArgumentException("cpu and memoryMb must be positive");
        }
        String tenantId = (tenant == null || tenant.isBlank()) ? "default" : tenant;
        log.info("submit received jobId={} tenant={} priority={} request=cpu:{},mem:{}",
                id, tenantId, priority, request.cpu(), request.memoryMb());

        synchronized (lock) {
            Job existing = jobs.get(id);
            if (existing != null) {
                String msg = "duplicate submission rejected, job already exists in state "
                        + existing.getState();
                log.warn("submit rejected jobId={} reason=DUPLICATE_JOB detail={}", id, msg);
                return SubmitResult.rejected(id, RejectReason.DUPLICATE_JOB, msg);
            }

            // 1) 单作业配额
            if (request.cpu() > props.getMaxJobCpu()
                    || request.memoryMb() > props.getMaxJobMemoryMb()) {
                return reject(id, tenantId, priority, request,
                        RejectReason.JOB_QUOTA_EXCEEDED,
                        String.format("request cpu:%d,mem:%d exceeds per-job quota cpu:%d,mem:%d",
                                request.cpu(), request.memoryMb(),
                                props.getMaxJobCpu(), props.getMaxJobMemoryMb()));
            }

            // 2) 物理容量：没有任何节点放得下，属于永久资源不足
            Resources biggest = biggestNodeCapacity();
            if (!request.fitsIn(biggest)) {
                return reject(id, tenantId, priority, request,
                        RejectReason.EXCEEDS_MAX_CAPACITY,
                        String.format("request cpu:%d,mem:%d exceeds largest node capacity cpu:%d,mem:%d",
                                request.cpu(), request.memoryMb(), biggest.cpu(), biggest.memoryMb()));
            }

            // 3) 租户配额（排队 + 运行中的申请量都计入，防止先排队后超配）
            Resources tenantUsed = tenantActiveUsage(tenantId);
            if (exceedsTenantQuota(tenantUsed.plus(request))) {
                return reject(id, tenantId, priority, request,
                        RejectReason.TENANT_QUOTA_EXCEEDED,
                        String.format("tenant=%s active usage cpu:%d,mem:%d plus request cpu:%d,mem:%d "
                                        + "exceeds quota cpu:%d,mem:%d",
                                tenantId, tenantUsed.cpu(), tenantUsed.memoryMb(),
                                request.cpu(), request.memoryMb(),
                                props.getTenantCpuQuota(), props.getTenantMemoryQuotaMb()));
            }

            Job job = new Job(id, tenantId, priority, request, ++sequenceGenerator);
            job.setSubmittedAt(Instant.now());
            job.addEvent("SUBMITTED", String.format(
                    "tenant=%s priority=%d request=cpu:%d,mem:%d",
                    tenantId, priority, request.cpu(), request.memoryMb()));
            jobs.put(id, job);

            if (props.isScheduleOnSubmit()) {
                dispatchPending();
            }
            log.info("submit accepted jobId={} state={}", id, job.getState());
            return SubmitResult.accepted(id, job.getState());
        }
    }

    private SubmitResult reject(String id, String tenant, int priority, Resources request,
                                RejectReason reason, String detail) {
        // 拒绝也是一种终态，登记下来以便查询具体原因；拒绝的作业不占用任何配额
        Job job = new Job(id, tenant, priority, request, ++sequenceGenerator);
        job.setState(JobState.REJECTED);
        job.setRejectReason(reason);
        job.setSubmittedAt(Instant.now());
        job.setFinishedAt(Instant.now());
        job.addEvent("REJECTED", reason + " - " + detail);
        jobs.put(id, job);
        log.warn("submit rejected jobId={} reason={} detail={}", id, reason, detail);
        return SubmitResult.rejected(id, reason, detail);
    }

    /** 请求取消作业；返回取消后的最新作业；终态作业返回 null 表示无法取消。 */
    public Job cancel(String jobId) {
        log.info("cancel requested jobId={}", jobId);
        synchronized (lock) {
            Job job = jobs.get(jobId);
            if (job == null) {
                return null;
            }
            switch (job.getState()) {
                case PENDING -> {
                    job.setState(JobState.CANCELLED);
                    job.setFinishedAt(Instant.now());
                    job.addEvent("CANCELLED", "cancelled while waiting in queue");
                    terminalOrder.add(job.getId());
                    log.info("job cancelled from queue jobId={}", jobId);
                    return job;
                }
                case RUNNING -> {
                    // 协作式取消：资源要等当前一次执行结束才释放，避免超卖
                    job.markCancelRequested();
                    job.addEvent("CANCEL_REQUESTED", "cancel requested while running; "
                            + "resources released when the current attempt finishes");
                    log.info("cancel marked on running job jobId={}", jobId);
                    return job;
                }
                default -> {
                    log.info("cancel ignored, job already terminal jobId={} state={}",
                            jobId, job.getState());
                    return null;
                }
            }
        }
    }

    public Job getJob(String jobId) {
        synchronized (lock) {
            return jobs.get(jobId);
        }
    }

    public List<Job> listJobs() {
        synchronized (lock) {
            return new ArrayList<>(jobs.values());
        }
    }

    public ClusterNode getNode(String nodeId) {
        synchronized (lock) {
            return nodes.get(nodeId);
        }
    }

    public List<ClusterNode> listNodes() {
        synchronized (lock) {
            return new ArrayList<>(nodes.values());
        }
    }

    /** 返回作业进入终态（成功/失败/取消）的确定性先后顺序的快照。 */
    public List<String> terminalOrder() {
        synchronized (lock) {
            return new ArrayList<>(terminalOrder);
        }
    }

    /**
     * 对全部 PENDING 作业做一轮分派。
     * 优先级高者先分派，同优先级按提交顺序(FIFO)；高优先级作业暂时放不下不会阻塞低优先级。
     *
     * @return 本轮成功分派的作业数
     */
    public int schedulePending() {
        synchronized (lock) {
            return dispatchPending();
        }
    }

    // 必须在持有 lock 时调用
    private int dispatchPending() {
        List<Job> pending = jobs.values().stream()
                .filter(j -> j.getState() == JobState.PENDING)
                .sorted(Comparator.comparingInt(Job::getPriority).reversed()
                        .thenComparingLong(Job::getSequence))
                .toList();

        int dispatched = 0;
        long now = System.currentTimeMillis();
        for (Job job : pending) {
            if (job.getRetryNotBeforeEpochMs() > now) {
                // 失败重试退避窗口内，本轮不参与分派（不记为资源不足）
                continue;
            }
            ClusterNode target = selectBestFit(job.getRequest());
            if (target != null) {
                allocateAndRun(job, target);
                dispatched++;
            } else {
                recordBlocked(job);
            }
        }
        return dispatched;
    }

    // 必须在持有 lock 时调用。best-fit：剩余 CPU 最少、其次剩余内存最少
    private ClusterNode selectBestFit(Resources request) {
        ClusterNode best = null;
        for (ClusterNode node : nodes.values()) {
            if (!node.canFit(request)) {
                continue;
            }
            if (best == null) {
                best = node;
                continue;
            }
            Resources a = node.getAvailable();
            Resources b = best.getAvailable();
            if (a.cpu() < b.cpu() || (a.cpu() == b.cpu() && a.memoryMb() < b.memoryMb())) {
                best = node;
            }
        }
        return best;
    }

    // 必须在持有 lock 时调用
    private void allocateAndRun(Job job, ClusterNode node) {
        node.allocate(job);
        job.setAssignedNodeId(node.getId());
        job.setState(JobState.RUNNING);
        job.setAttempts(job.getAttempts() + 1);
        job.setRetryNotBeforeEpochMs(0L);
        String reason = String.format(
                "dispatched by best-fit to %s (capacity cpu:%d,mem:%d; used cpu:%d,mem:%d; "
                        + "available after alloc cpu:%d,mem:%d); priority=%d queueSequence=%d attempt=%d",
                node.getId(),
                node.getCapacity().cpu(), node.getCapacity().memoryMb(),
                node.getUsed().cpu(), node.getUsed().memoryMb(),
                node.getAvailable().cpu(), node.getAvailable().memoryMb(),
                job.getPriority(), job.getSequence(), job.getAttempts());
        job.setLastDecision(new SchedulingDecision(Instant.now(), true, node.getId(), reason));
        job.setLastBlockReason(null);
        job.addEvent("DISPATCHED", reason);
        log.info("dispatch decision jobId={} scheduled=true node={} attempt={} request=cpu:{},mem:{}",
                job.getId(), node.getId(), job.getAttempts(),
                job.getRequest().cpu(), job.getRequest().memoryMb());
        workers.submit(() -> runAttempt(job));
    }

    // 必须在持有 lock 时调用
    private void recordBlocked(Job job) {
        ClusterNode roomiest = roomiestOnlineNode();
        String reason;
        if (roomiest == null) {
            reason = "no ONLINE node in cluster";
        } else {
            Resources avail = roomiest.getAvailable();
            reason = String.format(
                    "insufficient free resources right now; request cpu:%d,mem:%d, "
                            + "roomiest online node=%s available cpu:%d,mem:%d; will retry when resources free",
                    job.getRequest().cpu(), job.getRequest().memoryMb(),
                    roomiest.getId(), avail.cpu(), avail.memoryMb());
        }
        job.setLastDecision(new SchedulingDecision(Instant.now(), false, null, reason));
        // 仅当阻塞原因变化时追加事件，避免轮询刷审计日志
        if (!reason.equals(job.getLastBlockReason())) {
            job.addEvent("SCHEDULE_BLOCKED", reason);
        }
        job.setLastBlockReason(reason);
        log.info("dispatch decision jobId={} scheduled=false reason={}", job.getId(), reason);
    }

    // 工作线程：锁外执行，完成后回到锁内做一次且仅一次状态流转
    private void runAttempt(Job job) {
        boolean ok;
        try {
            ok = executionHandler.execute(job.getId());
        } catch (RuntimeException e) {
            log.warn("attempt threw jobId={} attempt={} error={}", job.getId(), job.getAttempts(), e.toString());
            ok = false;
        }

        synchronized (lock) {
            // 并发保护：只有当前确为 RUNNING 且仍占用节点的作业才能落地完成结果，
            // 杜绝取消与完成竞争导致的状态互相覆盖/重复释放
            if (job.getState() != JobState.RUNNING || job.getAssignedNodeId() == null) {
                log.info("stale completion ignored jobId={} state={}", job.getId(), job.getState());
                return;
            }
            ClusterNode node = nodes.get(job.getAssignedNodeId());
            if (node != null) {
                node.release(job);
            }
            String nodeId = job.getAssignedNodeId();
            job.setAssignedNodeId(null);

            if (job.isCancelRequested()) {
                job.setState(JobState.CANCELLED);
                job.setFinishedAt(Instant.now());
                job.addEvent("CANCELLED", "cancelled after running attempt finished on " + nodeId);
                terminalOrder.add(job.getId());
                log.info("job cancelled after attempt jobId={} node={}", job.getId(), nodeId);
                dispatchPending();
                return;
            }

            if (ok) {
                job.setState(JobState.SUCCEEDED);
                job.setFinishedAt(Instant.now());
                job.addEvent("SUCCEEDED", "attempt " + job.getAttempts() + " succeeded on " + nodeId);
                terminalOrder.add(job.getId());
                log.info("job succeeded jobId={} attempts={} node={}", job.getId(), job.getAttempts(), nodeId);
                dispatchPending();
                return;
            }

            int max = Math.max(1, props.getMaxAttempts());
            if (job.getAttempts() >= max) {
                job.setState(JobState.FAILED);
                job.setFinishedAt(Instant.now());
                job.addEvent("FAILED", "attempt " + job.getAttempts()
                        + " failed; max attempts " + max + " exhausted, entering terminal state FAILED");
                terminalOrder.add(job.getId());
                log.warn("job failed terminally jobId={} attempts={}/{}", job.getId(), job.getAttempts(), max);
                dispatchPending();
            } else {
                job.setState(JobState.PENDING);
                job.addEvent("ATTEMPT_FAILED",
                        "attempt " + job.getAttempts() + " failed on " + nodeId
                                + "; released resources, queued for retry "
                                + (job.getAttempts() + 1) + "/" + max);
                log.warn("attempt failed, will retry jobId={} attempts={}/{}",
                        job.getId(), job.getAttempts(), max);
                long delay = Math.max(0L, props.getRetryBackoffMs()) * job.getAttempts();
                job.setRetryNotBeforeEpochMs(System.currentTimeMillis() + delay);
                retryScheduler.schedule(() -> {
                    synchronized (lock) {
                        if (job.getState() == JobState.PENDING) {
                            dispatchPending();
                        }
                    }
                }, delay, TimeUnit.MILLISECONDS);
                // 失败后资源已释放，立刻尝试分派其他/本作业（受退避外的资源机会影响）
                dispatchPending();
            }
        }
    }

    // 必须在持有 lock 时调用
    private Resources tenantActiveUsage(String tenant) {
        long cpu = 0;
        long mem = 0;
        for (Job job : jobs.values()) {
            JobState s = job.getState();
            if (job.getTenant().equals(tenant)
                    && (s == JobState.PENDING || s == JobState.RUNNING)) {
                cpu += job.getRequest().cpu();
                mem += job.getRequest().memoryMb();
            }
        }
        return new Resources(cpu, mem);
    }

    // 必须在持有 lock 时调用；配额为 0 表示该维不限
    private boolean exceedsTenantQuota(Resources usage) {
        return (props.getTenantCpuQuota() > 0 && usage.cpu() > props.getTenantCpuQuota())
                || (props.getTenantMemoryQuotaMb() > 0 && usage.memoryMb() > props.getTenantMemoryQuotaMb());
    }

    // 必须在持有 lock 时调用
    private Resources biggestNodeCapacity() {
        long cpu = 0;
        long mem = 0;
        for (ClusterNode node : nodes.values()) {
            if (node.getState() == NodeState.ONLINE) {
                cpu = Math.max(cpu, node.getCapacity().cpu());
                mem = Math.max(mem, node.getCapacity().memoryMb());
            }
        }
        return new Resources(cpu, mem);
    }

    // 必须在持有 lock 时调用
    private ClusterNode roomiestOnlineNode() {
        ClusterNode best = null;
        for (ClusterNode node : nodes.values()) {
            if (node.getState() != NodeState.ONLINE) {
                continue;
            }
            if (best == null || node.getAvailable().cpu() > best.getAvailable().cpu()
                    || (node.getAvailable().cpu() == best.getAvailable().cpu()
                    && node.getAvailable().memoryMb() > best.getAvailable().memoryMb())) {
                best = node;
            }
        }
        return best;
    }

    public void shutdown() {
        workers.shutdownNow();
        retryScheduler.shutdownNow();
    }
}
