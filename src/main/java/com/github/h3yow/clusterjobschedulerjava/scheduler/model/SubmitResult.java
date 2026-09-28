package com.github.h3yow.clusterjobschedulerjava.scheduler.model;

/** 提交结果：接受 / 幂等重复 / 拒绝（含原因）。 */
public record SubmitResult(
        boolean accepted,
        boolean duplicate,
        String jobId,
        JobState state,
        RejectReason rejectReason,
        String message
) {

    public static SubmitResult accepted(JobState state, String jobId, String message) {
        return new SubmitResult(true, false, jobId, state, null, message);
    }

    /** 同一 jobId 的重复提交：既不算接受也不算拒绝，返回首次提交后的当前状态。 */
    public static SubmitResult duplicate(JobState state, String jobId, String message) {
        return new SubmitResult(false, true, jobId, state, null, message);
    }

    public static SubmitResult rejected(RejectReason reason, String jobId, String message) {
        return new SubmitResult(false, false, jobId, null, reason, message);
    }
}
