package com.github.h3yow.clusterjobschedulerjava.scheduler;

import com.github.h3yow.clusterjobschedulerjava.model.JobState;
import com.github.h3yow.clusterjobschedulerjava.model.RejectReason;

/**
 * 提交结果：accepted=false 时 rejectReason 给出明确拒绝原因。
 */
public record SubmitResult(boolean accepted, String jobId, JobState state,
                           RejectReason rejectReason, String message) {

    public static SubmitResult accepted(String jobId, JobState state) {
        return new SubmitResult(true, jobId, state, null, "accepted");
    }

    public static SubmitResult rejected(String jobId, RejectReason reason, String message) {
        return new SubmitResult(false, jobId, JobState.REJECTED, reason, message);
    }
}
