package com.github.h3yow.clusterjobschedulerjava.web.dto;

import com.github.h3yow.clusterjobschedulerjava.model.ClusterNode;

import java.util.List;

/**
 * 节点状态视图。
 */
public record NodeView(String id, String state, long capacityCpu, long capacityMemoryMb,
                       long usedCpu, long usedMemoryMb, long availableCpu,
                       long availableMemoryMb, List<String> runningJobIds) {

    public static NodeView from(ClusterNode node) {
        return new NodeView(
                node.getId(),
                node.getState().name(),
                node.getCapacity().cpu(),
                node.getCapacity().memoryMb(),
                node.getUsed().cpu(),
                node.getUsed().memoryMb(),
                node.getAvailable().cpu(),
                node.getAvailable().memoryMb(),
                List.copyOf(node.getRunningJobIds()));
    }
}
