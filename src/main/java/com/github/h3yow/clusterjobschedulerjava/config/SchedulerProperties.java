package com.github.h3yow.clusterjobschedulerjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 调度器配置项，前缀 scheduler。
 */
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    /** 单个作业允许申请的最大 CPU。 */
    private long maxJobCpu = 8;
    /** 单个作业允许申请的最大内存(MB)。 */
    private long maxJobMemoryMb = 16384;
    /** 单租户在集群内（排队+运行）允许占用的 CPU 配额。0 表示不限。 */
    private long tenantCpuQuota = 16;
    /** 单租户在集群内（排队+运行）允许占用的内存配额(MB)。0 表示不限。 */
    private long tenantMemoryQuotaMb = 32768;
    /** 每个作业的最大尝试次数（含首次执行）。 */
    private int maxAttempts = 3;
    /** 失败后重试的基础延迟(ms)。 */
    private long retryBackoffMs = 50;
    /** 是否在提交后立即尝试调度。 */
    private boolean scheduleOnSubmit = true;
    /** 初始化节点：逗号分隔的 name:cpu:memoryMb。 */
    private String nodes = "node-1:8:16384,node-2:8:16384";

    public long getMaxJobCpu() { return maxJobCpu; }
    public void setMaxJobCpu(long maxJobCpu) { this.maxJobCpu = maxJobCpu; }
    public long getMaxJobMemoryMb() { return maxJobMemoryMb; }
    public void setMaxJobMemoryMb(long maxJobMemoryMb) { this.maxJobMemoryMb = maxJobMemoryMb; }
    public long getTenantCpuQuota() { return tenantCpuQuota; }
    public void setTenantCpuQuota(long tenantCpuQuota) { this.tenantCpuQuota = tenantCpuQuota; }
    public long getTenantMemoryQuotaMb() { return tenantMemoryQuotaMb; }
    public void setTenantMemoryQuotaMb(long tenantMemoryQuotaMb) { this.tenantMemoryQuotaMb = tenantMemoryQuotaMb; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getRetryBackoffMs() { return retryBackoffMs; }
    public void setRetryBackoffMs(long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; }
    public boolean isScheduleOnSubmit() { return scheduleOnSubmit; }
    public void setScheduleOnSubmit(boolean scheduleOnSubmit) { this.scheduleOnSubmit = scheduleOnSubmit; }
    public String getNodes() { return nodes; }
    public void setNodes(String nodes) { this.nodes = nodes; }
}
