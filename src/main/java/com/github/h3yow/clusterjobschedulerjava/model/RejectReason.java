package com.github.h3yow.clusterjobschedulerjava.model;

/**
 * 提交被拒绝的明确原因。
 */
public enum RejectReason {
    DUPLICATE_JOB,
    JOB_QUOTA_EXCEEDED,
    TENANT_QUOTA_EXCEEDED,
    EXCEEDS_MAX_CAPACITY
}
