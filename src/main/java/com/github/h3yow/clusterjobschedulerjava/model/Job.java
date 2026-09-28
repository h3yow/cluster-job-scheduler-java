package com.github.h3yow.clusterjobschedulerjava.model;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 作业实体：保存声明信息、运行时状态、重试计数与审计轨迹。
 */
public class Job {

    private final String id;
    private final String tenant;
    private final int priority;
    private final Resources request;
    private final long sequence;

    private volatile JobState state;
    private volatile String assignedNodeId;
    private volatile int attempts;
    private volatile Instant submittedAt;
    private volatile Instant finishedAt;
    private volatile RejectReason rejectReason;
    private volatile String lastBlockReason;
    private volatile SchedulingDecision lastDecision;
    private volatile boolean cancelRequested;
    private volatile long retryNotBeforeEpochMs;
    private final List<JobEvent> events = new CopyOnWriteArrayList<>();

    public Job(String id, String tenant, int priority, Resources request, long sequence) {
        this.id = id;
        this.tenant = tenant;
        this.priority = priority;
        this.request = request;
        this.sequence = sequence;
        this.state = JobState.PENDING;
    }

    /** 追加一条可解释的状态/决策事件。 */
    public void addEvent(String type, String detail) {
        events.add(new JobEvent(Instant.now(), type, detail));
    }

    public boolean isCancelRequested() { return cancelRequested; }

    /** 协作式取消标记：运行中的作业在当前一次尝试结束后生效。 */
    public void markCancelRequested() { this.cancelRequested = true; }

    public long getRetryNotBeforeEpochMs() { return retryNotBeforeEpochMs; }

    /** 失败重试退避：该时刻之前不参与分派。0 表示立即可分派。 */
    public void setRetryNotBeforeEpochMs(long retryNotBeforeEpochMs) {
        this.retryNotBeforeEpochMs = retryNotBeforeEpochMs;
    }

    public String getId() { return id; }
    public String getTenant() { return tenant; }
    public int getPriority() { return priority; }
    public Resources getRequest() { return request; }
    public long getSequence() { return sequence; }
    public JobState getState() { return state; }
    public void setState(JobState state) { this.state = state; }
    public String getAssignedNodeId() { return assignedNodeId; }
    public void setAssignedNodeId(String assignedNodeId) { this.assignedNodeId = assignedNodeId; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public Instant getSubmittedAt() { return submittedAt; }
    public void setSubmittedAt(Instant submittedAt) { this.submittedAt = submittedAt; }
    public Instant getFinishedAt() { return finishedAt; }
    public void setFinishedAt(Instant finishedAt) { this.finishedAt = finishedAt; }
    public RejectReason getRejectReason() { return rejectReason; }
    public void setRejectReason(RejectReason rejectReason) { this.rejectReason = rejectReason; }
    public String getLastBlockReason() { return lastBlockReason; }
    public void setLastBlockReason(String lastBlockReason) { this.lastBlockReason = lastBlockReason; }
    public SchedulingDecision getLastDecision() { return lastDecision; }
    public void setLastDecision(SchedulingDecision lastDecision) { this.lastDecision = lastDecision; }
    public List<JobEvent> getEvents() { return events; }
}
