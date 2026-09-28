package com.github.h3yow.clusterjobschedulerjava.scheduler.engine;

import com.github.h3yow.clusterjobschedulerjava.scheduler.cluster.Cluster;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.ClusterNode;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.Job;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.CompletionOutcome;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobPriority;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobRequest;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobState;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.RejectReason;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.Resources;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.SubmitResult;
import com.github.h3yow.clusterjobschedulerjava.scheduler.quota.QuotaManager;
import com.github.h3yow.clusterjobschedulerjava.scheduler.quota.QuotaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 调度核心。
 *
 * <p>并发模型：单把公平锁 {@link #lock} 串行化全部状态变更（提交 / 取消 / 完成回报 /
 * 分派循环）。“资源检查 + 占位”“状态读取 + 状态写入”都在锁内一次完成，因此并发下不会
 * 超卖、不会重复占位、不会出现状态互相覆盖。锁内不做阻塞等待。</p>
 *
 * <p>分派规则：优先级（HIGH &gt; NORMAL &gt; LOW）优先，同优先级按提交时间 FIFO；
 * 节点使用 FIRST_FIT。资源暂时不足的高优作业会阻塞同节点适配的后续低优作业，
 * 但不同资源形状的作业仍可在同一轮插空分派。</p>
 *
 * <p>重试：执行失败后 attempt+1 重新入队；attempt 达到 maxAttempts 后进入 FAILED 终态。
 * 终态（SUCCEEDED / FAILED / CANCELLED / REJECTED）不可再变更。</p>
 */
public final class SchedulerCore {
    private static final Logger log = LoggerFactory.getLogger(SchedulerCore.class);

    /** 同优先级内按提交时间 FIFO。 */
    private static final Comparator<Job> QUEUE_ORDER = Comparator
            .comparingInt((Job j) -> j.getPriority().order())
            .thenComparingLong(Job::getSubmittedAtMs)
            .thenComparing(Job::getId);

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Map<String, Job> jobs = new LinkedHashMap<>();

    private final Cluster cluster;
    private final QuotaManager quotaManager;
    private final int defaultMaxAttempts;
    private final int maxQueueCapacity;
    private final Supplier<Long> clock;

    public SchedulerCore(Cluster cluster, QuotaManager quotaManager,
                         int defaultMaxAttempts, int maxQueueCapacity) {
        this(cluster, quotaManager, defaultMaxAttempts, maxQueueCapacity, System::currentTimeMillis);
    }

    SchedulerCore(Cluster cluster, QuotaManager quotaManager,
                  int defaultMaxAttempts, int maxQueueCapacity, Supplier<Long> clock) {
        if (defaultMaxAttempts < 1) {
            throw new IllegalArgumentException("defaultMaxAttempts must be >= 1");
        }
        this.cluster = cluster;
        this.quotaManager = quotaManager;
        this.defaultMaxAttempts = defaultMaxAttempts;
        this.maxQueueCapacity = maxQueueCapacity;
        this.clock = clock;
    }

    // ============================ 提交 ============================

    public SubmitResult submit(JobRequest request) {
        if (request == null) {
            return SubmitResult.rejected(RejectReason.INVALID_ARGUMENT, null, "request is null");
        }
        String validationError = validate(request);
        if (validationError != null) {
            log.info("submit rejected jobId={} reason={} detail={}",
                    request.jobId(), RejectReason.INVALID_ARGUMENT, validationError);
            return SubmitResult.rejected(RejectReason.INVALID_ARGUMENT, request.jobId(), validationError);
        }

        lock.lock();
        try {
            Job existing = jobs.get(request.jobId());
            if (existing != null) {
                // 幂等：重复提交不产生第二次执行，返回首次提交后的当前状态。
                log.info("submit duplicate jobId={} currentState={}", request.jobId(), existing.getState());
                return SubmitResult.duplicate(existing.getState(), request.jobId(),
                        "job already submitted; current state=" + existing.getState());
            }

            QuotaRecord quota = quotaManager.quotaOf(request.tenant());
            TenantUsage usage = usageOf(request.tenant());
            if (usage.activeJobs >= quota.maxJobs()
                    || !quota.maxResources().canFit(usage.usedResources.plus(request.resources()))) {
                String detail = "tenant=" + request.tenant()
                        + " quota{maxJobs=" + quota.maxJobs()
                        + ",maxResources=" + quota.maxResources() + "}"
                        + " currentUsage{activeJobs=" + usage.activeJobs
                        + ",used=" + usage.usedResources + "}"
                        + " requested=" + request.resources();
                return rejectAndStore(request, RejectReason.QUOTA_EXCEEDED, detail);
            }

            if (request.resources().getCpuCores() > cluster.totalCapacity().getCpuCores()
                    || request.resources().getMemoryMb() > cluster.totalCapacity().getMemoryMb()) {
                String detail = "requested=" + request.resources()
                        + " exceeds clusterTotalCapacity=" + cluster.totalCapacity();
                return rejectAndStore(request, RejectReason.INSUFFICIENT_CAPACITY, detail);
            }

            long queuedActive = jobs.values().stream()
                    .filter(j -> j.getState() == JobState.QUEUED).count();
            if (queuedActive >= maxQueueCapacity) {
                return rejectAndStore(request, RejectReason.QUEUE_FULL,
                        "queue is full, capacity=" + maxQueueCapacity + ", queued=" + queuedActive);
            }

            Job job = new Job(request, defaultMaxAttempts, clock.get());
            jobs.put(job.getId(), job);
            log.info("submit accepted jobId={} tenant={} priority={} required={} maxAttempts={}",
                    job.getId(), job.getTenant(), job.getPriority(), job.getRequired(), job.getMaxAttempts());

            schedulePass();
            return SubmitResult.accepted(job.getState(), job.getId(),
                    "accepted; state=" + job.getState()
                            + (job.getAssignedNodeId() != null ? ", node=" + job.getAssignedNodeId() : ""));
        } finally {
            lock.unlock();
        }
    }

    private static String validate(JobRequest r) {
        if (r.jobId() == null || r.jobId().isBlank()) {
            return "jobId is blank";
        }
        if (r.tenant() == null || r.tenant().isBlank()) {
            return "tenant is blank";
        }
        if (r.resources() == null) {
            return "resources is null";
        }
        if (r.resources().getCpuCores() <= 0 || r.resources().getMemoryMb() <= 0) {
            return "resources must be positive: " + r.resources();
        }
        if (r.maxAttempts() != null && r.maxAttempts() < 1) {
            return "maxAttempts must be >= 1: " + r.maxAttempts();
        }
        return null;
    }

    // ============================ 取消 ============================

    public boolean cancel(String jobId, String reason) {
        lock.lock();
        try {
            Job job = jobs.get(jobId);
            if (job == null) {
                log.info("cancel ignored jobId={} reason=NOT_FOUND", jobId);
                return false;
            }
            if (job.getState().isTerminal()) {
                log.info("cancel ignored jobId={} reason=ALREADY_TERMINAL state={}", jobId, job.getState());
                return false;
            }
            String nodeId = job.getAssignedNodeId();
            if (nodeId != null) {
                releaseOnNode(job, nodeId);
            }
            job.markCancelled(reason == null ? "cancelled" : reason, clock.get());
            log.info("cancel jobId={} state=CANCELLED node={} detail={}", jobId, nodeId, reason);
            schedulePass();
            return true;
        } finally {
            lock.unlock();
        }
    }

    // ============================ 完成回报 / 重试 ============================

    public boolean reportCompletion(String jobId, CompletionOutcome outcome, String detail) {
        if (outcome == null) {
            throw new IllegalArgumentException("outcome is null");
        }
        lock.lock();
        try {
            Job job = jobs.get(jobId);
            if (job == null) {
                log.info("completion ignored jobId={} reason=NOT_FOUND outcome={}", jobId, outcome);
                return false;
            }
            if (job.getState() != JobState.RUNNING) {
                // 例如作业在执行期间被取消：重复 / 迟到的回报一律忽略，绝不覆盖终态。
                log.info("completion ignored jobId={} outcome={} currentState={} reason=NOT_RUNNING",
                        jobId, outcome, job.getState());
                return false;
            }

            String nodeId = job.getAssignedNodeId();
            releaseOnNode(job, nodeId);

            if (outcome == CompletionOutcome.SUCCESS) {
                job.markSucceeded(clock.get());
                log.info("completion jobId={} state=SUCCEEDED node={} attempts={}",
                        jobId, nodeId, job.getAttempt());
            } else if (job.getAttempt() < job.getMaxAttempts()) {
                int nextAttempt = job.getAttempt() + 1;
                job.markQueuedAfterFailure(nextAttempt, detail, clock.get());
                log.info("completion jobId={} outcome=FAILURE node={} requeue attempt={}/{} detail={}",
                        jobId, nodeId, nextAttempt, job.getMaxAttempts(), detail);
            } else {
                job.markFailed("max attempts (" + job.getMaxAttempts() + ") exhausted; lastError="
                        + detail, clock.get());
                log.info("completion jobId={} state=FAILED attempts={}/{} detail={}",
                        jobId, job.getAttempt(), job.getMaxAttempts(), detail);
            }

            schedulePass();
            return true;
        } finally {
            lock.unlock();
        }
    }

    // ============================ 分派循环 ============================

    public int schedulePending() {
        lock.lock();
        try {
            return schedulePass();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 单轮分派。必须在持有锁时调用。
     *
     * <p>单轮扫描即可表达“高优先级占住而资源形状不同的低优插空”：每跳过一个作业就继续
     * 检查后面的作业；若发生过分派则资源视图变化，再跑一轮，直到没有新分派。</p>
     */
    private int schedulePass() {
        int totalAssigned = 0;
        boolean assignedInRound;
        do {
            assignedInRound = false;
            List<Job> pending = jobs.values().stream()
                    .filter(j -> j.getState() == JobState.QUEUED)
                    .sorted(QUEUE_ORDER)
                    .collect(Collectors.toList());

            for (Job job : pending) {
                if (job.getState() != JobState.QUEUED) {
                    continue; // 本轮内已被处理（当前实现分派后状态即变更，防御性检查）
                }
                ClusterNode node = cluster.findFit(job.getRequired());
                if (node == null) {
                    String why = "no node can fit " + job.getRequired()
                            + " right now; clusterAvailable=" + cluster.totalAvailable();
                    job.recordScheduleSkip(why, clock.get());
                    log.info("schedule skip jobId={} priority={} required={} reason={}",
                            job.getId(), job.getPriority(), job.getRequired(), why);
                    continue;
                }
                // findFit 与 tryAllocate 同处一把锁内，资源检查不会被其他线程穿插。
                if (!node.tryAllocate(job.getId(), job.getRequired())) {
                    String why = "allocation raced on node=" + node.getId();
                    job.recordScheduleSkip(why, clock.get());
                    continue;
                }
                job.markAssigned(node.getId(), clock.get());
                assignedInRound = true;
                totalAssigned++;
                log.info("schedule assign jobId={} priority={} required={} node={} nodeAvailableAfter={}",
                        job.getId(), job.getPriority(), job.getRequired(), node.getId(), node.available());
            }
        } while (assignedInRound);
        return totalAssigned;
    }

    // ============================ 查询 ============================

    public Optional<Job> getJob(String jobId) {
        lock.lock();
        try {
            return Optional.ofNullable(jobs.get(jobId));
        } finally {
            lock.unlock();
        }
    }

    /** 快照列表，避免锁外遍历内部集合。 */
    public List<Job> listJobs() {
        lock.lock();
        try {
            return new ArrayList<>(jobs.values());
        } finally {
            lock.unlock();
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    // ============================ 内部工具 ============================

    /**
     * 将被拒绝的作业以 REJECTED 终态入库：不占配额、不进队列、永远不会执行，
     * 但之后可通过 getJob 查询到拒绝原因与事件；同一 jobId 再次提交命中幂等分支。
     * 必须在持有锁时调用。
     */
    private SubmitResult rejectAndStore(JobRequest request, RejectReason reason, String detail) {
        Job job = new Job(request, defaultMaxAttempts, clock.get());
        job.markRejected(reason, detail, clock.get());
        jobs.put(job.getId(), job);
        log.info("submit rejected jobId={} tenant={} reason={} detail={}",
                request.jobId(), request.tenant(), reason, detail);
        return SubmitResult.rejected(reason, request.jobId(), detail);
    }

    private void releaseOnNode(Job job, String nodeId) {
        for (ClusterNode node : cluster.nodes()) {
            if (node.getId().equals(nodeId)) {
                boolean released = node.release(job.getId(), job.getRequired());
                if (!released) {
                    log.warn("release mismatch jobId={} node={} (job not registered on node)",
                            job.getId(), nodeId);
                }
                return;
            }
        }
        log.warn("release skipped jobId={} node={} (node not found)", job.getId(), nodeId);
    }

    /** 统计某租户当前“保活”（QUEUED + RUNNING）的作业数与资源占用。 */
    private TenantUsage usageOf(String tenant) {
        int active = 0;
        Resources used = Resources.ZERO;
        for (Job job : jobs.values()) {
            if (tenant.equals(job.getTenant())
                    && (job.getState() == JobState.QUEUED || job.getState() == JobState.RUNNING)) {
                active++;
                used = used.plus(job.getRequired());
            }
        }
        return new TenantUsage(active, used);
    }

    private record TenantUsage(int activeJobs, Resources usedResources) {
    }
}
