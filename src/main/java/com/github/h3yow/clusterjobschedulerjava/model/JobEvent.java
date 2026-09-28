package com.github.h3yow.clusterjobschedulerjava.model;

import java.time.Instant;

/**
 * 作业状态流转/调度决策事件，用于可解释的审计轨迹。
 */
public record JobEvent(Instant time, String type, String detail) {
}
