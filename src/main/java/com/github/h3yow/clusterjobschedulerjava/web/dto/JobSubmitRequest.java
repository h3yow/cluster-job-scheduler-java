package com.github.h3yow.clusterjobschedulerjava.web.dto;

/**
 * 作业提交请求。
 */
public record JobSubmitRequest(String id, String tenant, Integer priority, Long cpu, Long memoryMb) {
}
