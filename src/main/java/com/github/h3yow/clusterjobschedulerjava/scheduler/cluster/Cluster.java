package com.github.h3yow.clusterjobschedulerjava.scheduler.cluster;

import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.ClusterNode;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.Resources;

import java.util.Collections;
import java.util.List;

/** 节点集合与容量聚合视图。 */
public final class Cluster {
    private final List<ClusterNode> nodes;

    public Cluster(List<ClusterNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("cluster must contain at least one node");
        }
        this.nodes = List.copyOf(nodes);
    }

    public List<ClusterNode> nodes() {
        return nodes;
    }

    /** 全集群总容量。 */
    public Resources totalCapacity() {
        Resources total = Resources.ZERO;
        for (ClusterNode node : nodes) {
            total = total.plus(node.getCapacity());
        }
        return total;
    }

    /** 全集群剩余可用资源。 */
    public Resources totalAvailable() {
        Resources total = Resources.ZERO;
        for (ClusterNode node : nodes) {
            total = total.plus(node.available());
        }
        return total;
    }

    /**
     * FIRST_FIT：按节点配置顺序返回第一个剩余资源满足 request 的节点；没有则 null。
     * 调用方持有全局调度锁，保证后续 tryAllocate 与本次检查之间没有穿插的占用。
     */
    public ClusterNode findFit(Resources request) {
        for (ClusterNode node : nodes) {
            if (node.available().canFit(request)) {
                return node;
            }
        }
        return null;
    }
}
