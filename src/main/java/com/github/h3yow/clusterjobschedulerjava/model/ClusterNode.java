package com.github.h3yow.clusterjobschedulerjava.model;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 集群节点：维护总容量、已分配量与在其上运行的作业。
 * 所有变更由调度器在统一锁内完成，节点方法本身不做并发控制。
 */
public class ClusterNode {

    private final String id;
    private final Resources capacity;
    private Resources used;
    private NodeState state = NodeState.ONLINE;
    private final Set<String> runningJobIds = new LinkedHashSet<>();

    public ClusterNode(String id, Resources capacity) {
        this.id = id;
        this.capacity = capacity;
        this.used = Resources.ZERO;
    }

    public String getId() { return id; }
    public Resources getCapacity() { return capacity; }
    public Resources getUsed() { return used; }
    public Resources getAvailable() {
        return capacity.minus(used);
    }

    public NodeState getState() { return state; }
    public void setState(NodeState state) { this.state = state; }
    public Set<String> getRunningJobIds() { return runningJobIds; }

    /** 在线且剩余资源（两维）足够时可放入。 */
    public boolean canFit(Resources request) {
        return state == NodeState.ONLINE && request.fitsIn(getAvailable());
    }

    /** 占位：调用方必须已通过 canFit 校验并持有调度锁。 */
    public void allocate(Job job) {
        if (!canFit(job.getRequest())) {
            throw new IllegalStateException("node " + id + " cannot fit job " + job.getId());
        }
        used = used.plus(job.getRequest());
        runningJobIds.add(job.getId());
    }

    /** 释放：幂等，作业不在该节点上时不产生效果。 */
    public void release(Job job) {
        if (runningJobIds.remove(job.getId())) {
            used = used.minus(job.getRequest());
        }
    }
}
