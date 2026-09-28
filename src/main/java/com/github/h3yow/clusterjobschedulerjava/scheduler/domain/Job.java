package com.github.h3yow.clusterjobschedulerjava.scheduler.domain;

import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobEvent;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobPriority;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobRequest;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobState;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.RejectReason;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.Resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 作业运行时实体。所有读写必须在调度器锁内完成。 */
public final class Job {
    private final String id;
    private final String tenant;
    private final String name;
    private final Resources required;
    private final JobPriority priority;
    private final int maxAttempts;
    private final long submittedAtMs;

    private JobState state = JobState.QUEUED;
    private int attempt;
    private String assignedNodeId;
    private RejectReason rejectReason;
    private String terminalReason;
    private String lastScheduleSkip;
    private long lastScheduleSkipMs;
    private final List<JobEvent> events = new ArrayList<>();

    public Job(JobRequest request, int defaultMaxAttempts, long nowMs) {
        this.id = request.jobId();
        this.tenant = request.tenant();
        this.name = request.name();
        this.required = request.resources();
        this.priority = request.priority() == null ? JobPriority.NORMAL : request.priority();
        this.maxAttempts = request.maxAttempts() != null ? request.maxAttempts() : defaultMaxAttempts;
        this.submittedAtMs = nowMs;
        this.attempt = 1;
        appendEvent("SUBMITTED",
                "tenant=" + tenant + ", required=" + required + ", priority=" + priority
                        + ", maxAttempts=" + maxAttempts, nowMs);
    }

    public String getId() { return id; }
    public String getTenant() { return tenant; }
    public String getName() { return name; }
    public Resources getRequired() { return required; }
    public JobPriority getPriority() { return priority; }
    public int getMaxAttempts() { return maxAttempts; }
    public long getSubmittedAtMs() { return submittedAtMs; }
    public JobState getState() { return state; }
    public int getAttempt() { return attempt; }
    public String getAssignedNodeId() { return assignedNodeId; }
    public RejectReason getRejectReason() { return rejectReason; }
    public String getTerminalReason() { return terminalReason; }
    public String getLastScheduleSkip() { return lastScheduleSkip; }
    public long getLastScheduleSkipMs() { return lastScheduleSkipMs; }
    public List<JobEvent> getEvents() { return Collections.unmodifiableList(events); }

    /** QUEUED -> RUNNING：占位成功后调用。 */
    public void markAssigned(String nodeId, long nowMs) {
        if (state != JobState.QUEUED) {
            throw new IllegalStateException("job " + id + " is not QUEUED: " + state);
        }
        this.state = JobState.RUNNING;
        this.assignedNodeId = nodeId;
        this.lastScheduleSkip = null;
        appendEvent("ASSIGNED", "node=" + nodeId + ", attempt=" + attempt, nowMs);
    }

    /**
     * 记录本轮调度未能分派的原因。只在原因发生变化时追加事件，
     * 既保证每次调度判定可追溯，也避免轮询刷爆事件列表。
     */
    public void recordScheduleSkip(String reason, long nowMs) {
        this.lastScheduleSkip = reason;
        this.lastScheduleSkipMs = nowMs;
        if (events.isEmpty()
                || !"SCHEDULE_SKIPPED".equals(events.get(events.size() - 1).type())
                || !reason.equals(events.get(events.size() - 1).detail())) {
            appendEvent("SCHEDULE_SKIPPED", reason, nowMs);
        }
    }

    /** RUNNING -> QUEUED：本次执行失败但仍可重试。 */
    public void markQueuedAfterFailure(int newAttempt, String reason, long nowMs) {
        if (state != JobState.RUNNING) {
            throw new IllegalStateException("job " + id + " is not RUNNING: " + state);
        }
        this.state = JobState.QUEUED;
        this.attempt = newAttempt;
        this.assignedNodeId = null;
        this.lastScheduleSkip = null;
        appendEvent("RETRY_QUEUED",
                "reason=" + reason + ", nextAttempt=" + newAttempt + "/" + maxAttempts, nowMs);
    }

    /** RUNNING -> SUCCEEDED。 */
    public void markSucceeded(long nowMs) {
        if (state != JobState.RUNNING) {
            throw new IllegalStateException("job " + id + " is not RUNNING: " + state);
        }
        this.state = JobState.SUCCEEDED;
        this.terminalReason = "succeeded after attempt " + attempt;
        appendEvent("SUCCEEDED", "attempt=" + attempt, nowMs);
    }

    /** 任意活动态 -> FAILED（重试耗尽）。 */
    public void markFailed(String reason, long nowMs) {
        if (state.isTerminal()) {
            throw new IllegalStateException("job " + id + " already terminal: " + state);
        }
        this.state = JobState.FAILED;
        this.terminalReason = reason;
        this.assignedNodeId = null;
        appendEvent("FAILED", reason + ", attemptsUsed=" + attempt + "/" + maxAttempts, nowMs);
    }

    /** QUEUED/RUNNING -> CANCELLED。 */
    public void markCancelled(String reason, long nowMs) {
        if (state.isTerminal()) {
            throw new IllegalStateException("job " + id + " already terminal: " + state);
        }
        this.state = JobState.CANCELLED;
        this.terminalReason = reason;
        this.assignedNodeId = null;
        appendEvent("CANCELLED", reason, nowMs);
    }

    /** QUEUED（尚未首次分派）-> REJECTED，仅用于容量无法满足的拒绝场景。 */
    public void markRejected(RejectReason reason, String message, long nowMs) {
        this.state = JobState.REJECTED;
        this.rejectReason = reason;
        this.terminalReason = message;
        appendEvent("REJECTED", "reason=" + reason + ", " + message, nowMs);
    }

    public void appendEvent(String type, String detail, long nowMs) {
        events.add(new JobEvent(nowMs, type, detail));
    }
}
