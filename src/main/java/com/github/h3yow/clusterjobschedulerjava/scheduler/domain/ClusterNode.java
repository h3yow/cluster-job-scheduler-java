package com.github.h3yow.clusterjobschedulerjava.scheduler.domain;

import com.github.h3yow.clusterjobschedulerjava.scheduler.model.Resources;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** 进程内模拟的集群节点，维护自身总资源与已占用资源。 */
public final class ClusterNode {
    private final String id;
    private final Resources capacity;
    private Resources used = Resources.ZERO;
    private final Set<String> runningJobIds = new HashSet<>();

    public ClusterNode(String id, Resources capacity) {
        this.id = id;
        this.capacity = capacity;
    }

    public String getId() { return id; }
    public Resources getCapacity() { return capacity; }
    public Resources getUsed() { return used; }

    /** 剩余可分配资源。 */
    public Resources available() {
        return capacity.minus(used);
    }

    /**
     * 在本节点上为作业占位；资源不足或作业已在节点上时返回 false 且不产生任何变更。
     * 调用方需持有全局调度锁，保证“检查 + 占用”整体串行。
     */
    public boolean tryAllocate(String jobId, Resources request) {
        if (runningJobIds.contains(jobId)) {
            return false;
        }
        if (!available().canFit(request)) {
            return false;
        }
        used = used.plus(request);
        runningJobIds.add(jobId);
        return true;
    }

    /** 释放作业占用并归还资源；作业不在本节点返回 false 且不产生变更。 */
    public boolean release(String jobId, Resources request) {
        if (!runningJobIds.remove(jobId)) {
            return false;
        }
        used = used.minus(request);
        return true;
    }

    public Set<String> getRunningJobIds() {
        return Collections.unmodifiableSet(runningJobIds);
    }
}
