package com.github.h3yow.clusterjobschedulerjava.config;

import com.github.h3yow.clusterjobschedulerjava.scheduler.JobScheduler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 装配调度器：启用配置属性并初始化模拟集群节点。
 */
@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfig {

    private final JobScheduler scheduler;

    public SchedulerConfig(JobScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void init() {
        scheduler.initNodes();
    }

    @PreDestroy
    public void destroy() {
        scheduler.shutdown();
    }
}
