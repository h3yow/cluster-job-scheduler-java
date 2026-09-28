package com.github.h3yow.clusterjobschedulerjava.scheduler;

/**
 * 作业执行逻辑抽象。默认实现让作业成功；测试可注入失败行为。
 */
@FunctionalInterface
public interface JobExecutionHandler {

    /**
     * 执行一次作业。
     *
     * @param jobId 作业 id
     * @return true 表示成功，false 表示本次执行失败（可重试）
     */
    boolean execute(String jobId);
}
