package com.github.h3yow.clusterjobschedulerjava.scheduler.model;

/** 作业生命周期状态。 */
public enum JobState {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    REJECTED;

    /** 是否为不可再变更的终态。 */
    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED || this == REJECTED;
    }
}
