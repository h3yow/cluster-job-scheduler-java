package com.github.h3yow.clusterjobschedulerjava.scheduler.model;

/** 作业生命周期 / 调度决策事件，用于状态流转的可解释。 */
public record JobEvent(long timestampMs, String type, String detail) {

    public static JobEvent of(String type, String detail) {
        return new JobEvent(System.currentTimeMillis(), type, detail);
    }
}
