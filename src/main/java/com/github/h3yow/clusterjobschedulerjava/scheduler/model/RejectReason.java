package com.github.h3yow.clusterjobschedulerjava.scheduler.model;

/** 提交被拒绝时的可区分原因。 */
public enum RejectReason {
    /** 请求参数非法（id / 租户为空、资源非法等）。 */
    INVALID_ARGUMENT,
    /** 同一 jobId 已提交过（幂等返回，不计为新执行）。 */
    DUPLICATE_SUBMISSION,
    /** 超过租户的并发作业数或资源配额。 */
    QUOTA_EXCEEDED,
    /** 请求资源超过集群物理容量，任何时候都无法调度。 */
    INSUFFICIENT_CAPACITY,
    /** 等待队列已满。 */
    QUEUE_FULL
}
