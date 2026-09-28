package com.github.h3yow.clusterjobschedulerjava.scheduler.model;

/**
 * 作业提交请求。
 *
 * @param maxAttempts 允许空，表示使用全局默认最大尝试次数
 */
public record JobRequest(
        String jobId,
        String tenant,
        String name,
        Resources resources,
        JobPriority priority,
        Integer maxAttempts
) {
}
