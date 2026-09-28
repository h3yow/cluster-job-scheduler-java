package com.github.h3yow.clusterjobschedulerjava.scheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/** 调度器配置：scheduler.* 前缀。 */
@ConfigurationProperties(prefix = "scheduler")
public final class SchedulerProperties {

    private int defaultMaxAttempts = 3;
    private int maxQueueCapacity = 1000;
    private String nodeSelector = "FIRST_FIT";
    private final Map<String, TenantQuota> quotas = new LinkedHashMap<>();
    private final Cluster cluster = new Cluster();

    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int v) { this.defaultMaxAttempts = v; }

    public int getMaxQueueCapacity() { return maxQueueCapacity; }
    public void setMaxQueueCapacity(int v) { this.maxQueueCapacity = v; }

    public String getNodeSelector() { return nodeSelector; }
    public void setNodeSelector(String v) { this.nodeSelector = v; }

    public Map<String, TenantQuota> getQuotas() { return quotas; }

    public Cluster getCluster() { return cluster; }

    public static final class TenantQuota {
        private int maxJobs = 100;
        private long maxCpuCores = 100;
        private long maxMemoryMb = 102_400L;

        public int getMaxJobs() { return maxJobs; }
        public void setMaxJobs(int v) { this.maxJobs = v; }
        public long getMaxCpuCores() { return maxCpuCores; }
        public void setMaxCpuCores(long v) { this.maxCpuCores = v; }
        public long getMaxMemoryMb() { return maxMemoryMb; }
        public void setMaxMemoryMb(long v) { this.maxMemoryMb = v; }
    }

    public static final class Cluster {
        private final java.util.List<Node> nodes = new java.util.ArrayList<>();

        public java.util.List<Node> getNodes() { return nodes; }

        public static final class Node {
            private String id;
            private int cpuCores = 8;
            private long memoryMb = 16_384L;

            public String getId() { return id; }
            public void setId(String v) { this.id = v; }
            public int getCpuCores() { return cpuCores; }
            public void setCpuCores(int v) { this.cpuCores = v; }
            public long getMemoryMb() { return memoryMb; }
            public void setMemoryMb(long v) { this.memoryMb = v; }
        }
    }
}
