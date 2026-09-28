package com.github.h3yow.clusterjobschedulerjava.model;

/**
 * 作业生命周期状态。
 */
public enum JobState {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    REJECTED
}
