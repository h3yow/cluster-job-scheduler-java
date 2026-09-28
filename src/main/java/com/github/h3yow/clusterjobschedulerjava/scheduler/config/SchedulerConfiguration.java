package com.github.h3yow.clusterjobschedulerjava.scheduler.config;

import com.github.h3yow.clusterjobschedulerjava.scheduler.cluster.Cluster;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.ClusterNode;
import com.github.h3yow.clusterjobschedulerjava.scheduler.engine.SchedulerCore;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.Resources;
import com.github.h3yow.clusterjobschedulerjava.scheduler.quota.QuotaManager;
import com.github.h3yow.clusterjobschedulerjava.scheduler.quota.QuotaRecord;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 组装集群 / 配额 / 调度核心 Bean。 */
@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfiguration {

    /** 未显式配置节点时的内置演示集群：两个 8C/16384MB 节点。 */
    @Bean
    public Cluster cluster(SchedulerProperties properties) {
        List<ClusterNode> nodes;
        if (properties.getCluster().getNodes().isEmpty()) {
            nodes = List.of(
                    new ClusterNode("node-1", new Resources(8, 16_384L)),
                    new ClusterNode("node-2", new Resources(8, 16_384L))
            );
        } else {
            nodes = properties.getCluster().getNodes().stream()
                    .map(n -> new ClusterNode(n.getId(), new Resources(n.getCpuCores(), n.getMemoryMb())))
                    .toList();
        }
        return new Cluster(nodes);
    }

    @Bean
    public QuotaManager quotaManager(SchedulerProperties properties) {
        Map<String, QuotaRecord> quotas = new LinkedHashMap<>();
        properties.getQuotas().forEach((tenant, q) -> quotas.put(tenant,
                new QuotaRecord(q.getMaxJobs(),
                        new Resources((int) q.getMaxCpuCores(), q.getMaxMemoryMb()))));
        // 默认配额：未显式配置的租户使用较宽松的兜底值。
        QuotaRecord defaultQuota = new QuotaRecord(100, new Resources(64, 131_072L));
        return new QuotaManager(quotas, defaultQuota);
    }

    @Bean
    public SchedulerCore schedulerCore(Cluster cluster, QuotaManager quotaManager,
                                       SchedulerProperties properties) {
        return new SchedulerCore(cluster, quotaManager,
                properties.getDefaultMaxAttempts(), properties.getMaxQueueCapacity());
    }
}
