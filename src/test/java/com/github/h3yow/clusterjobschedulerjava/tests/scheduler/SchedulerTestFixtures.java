package com.github.h3yow.clusterjobschedulerjava.tests.scheduler;

import com.github.h3yow.clusterjobschedulerjava.scheduler.cluster.Cluster;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.ClusterNode;
import com.github.h3yow.clusterjobschedulerjava.scheduler.engine.SchedulerCore;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobPriority;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobRequest;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.Resources;
import com.github.h3yow.clusterjobschedulerjava.scheduler.quota.QuotaManager;
import com.github.h3yow.clusterjobschedulerjava.scheduler.quota.QuotaRecord;

import java.util.List;
import java.util.Map;

/** 测试夹具：构建可控的小集群与调度器。 */
final class SchedulerTestFixtures {

    private SchedulerTestFixtures() {
    }

    /** 默认：两个 8C/16GB 节点，共 16C/32GB。 */
    static Cluster twoNodeCluster() {
        return new Cluster(List.of(
                new ClusterNode("node-1", new Resources(8, 16_384L)),
                new ClusterNode("node-2", new Resources(8, 16_384L))
        ));
    }

    static Cluster singleNodeCluster(int cpu, long memoryMb) {
        return new Cluster(List.of(new ClusterNode("node-1", new Resources(cpu, memoryMb))));
    }

    static QuotaManager quotaManager(String tenant, int maxJobs, int maxCpu, long maxMemoryMb) {
        return new QuotaManager(
                Map.of(tenant, new QuotaRecord(maxJobs, new Resources(maxCpu, maxMemoryMb))),
                new QuotaRecord(100, new Resources(64, 131_072L)));
    }

    static SchedulerCore core(Cluster cluster, QuotaManager quotas, int defaultMaxAttempts) {
        return new SchedulerCore(cluster, quotas, defaultMaxAttempts, 1000);
    }

    static JobRequest request(String jobId, String tenant, int cpu, long memoryMb, JobPriority priority) {
        return new JobRequest(jobId, tenant, "job-" + jobId,
                new Resources(cpu, memoryMb), priority, null);
    }

    static JobRequest requestWithAttempts(String jobId, String tenant, int cpu, long memoryMb,
                                          JobPriority priority, int maxAttempts) {
        return new JobRequest(jobId, tenant, "job-" + jobId,
                new Resources(cpu, memoryMb), priority, maxAttempts);
    }
}
