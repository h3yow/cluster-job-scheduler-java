package com.github.h3yow.clusterjobschedulerjava.web.dto;

import com.github.h3yow.clusterjobschedulerjava.model.Job;
import com.github.h3yow.clusterjobschedulerjava.model.JobEvent;
import com.github.h3yow.clusterjobschedulerjava.model.RejectReason;
import com.github.h3yow.clusterjobschedulerjava.model.SchedulingDecision;

import java.time.Instant;
import java.util.List;

/**
 * 作业状态视图（含拒绝/阻塞原因与审计轨迹）。
 */
public record JobView(String id, String tenant, int priority, long cpu, long memoryMb,
                      String state, String assignedNodeId, int attempts,
                      RejectReason rejectReason, String blockReason,
                      SchedulingDecision lastDecision,
                      Instant submittedAt, Instant finishedAt,
                      List<JobEvent> events) {

    public static JobView from(Job job) {
        return new JobView(
                job.getId(),
                job.getTenant(),
                job.getPriority(),
                job.getRequest().cpu(),
                job.getRequest().memoryMb(),
                job.getState().name(),
                job.getAssignedNodeId(),
                job.getAttempts(),
                job.getRejectReason(),
                job.getLastBlockReason(),
                job.getLastDecision(),
                job.getSubmittedAt(),
                job.getFinishedAt(),
                List.copyOf(job.getEvents()));
    }
}
