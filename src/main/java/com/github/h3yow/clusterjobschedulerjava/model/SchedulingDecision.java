package com.github.h3yow.clusterjobschedulerjava.model;

import java.time.Instant;

/**
 * 一次调度判定结果。
 *
 * @param scheduled 是否成功分派
 * @param nodeId    分派到的节点；未分派为 null
 * @param reason    判定依据/无法调度原因
 */
public record SchedulingDecision(Instant time, boolean scheduled, String nodeId, String reason) {
}
