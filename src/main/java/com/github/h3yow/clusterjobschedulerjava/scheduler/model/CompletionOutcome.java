package com.github.h3yow.clusterjobschedulerjava.scheduler.model;

/** 执行回报：成功或失败（失败可能触发重试）。 */
public enum CompletionOutcome {
    SUCCESS,
    FAILURE
}
