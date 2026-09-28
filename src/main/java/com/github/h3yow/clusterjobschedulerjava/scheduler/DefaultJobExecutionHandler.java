package com.github.h3yow.clusterjobschedulerjava.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 默认执行器：模拟一段本地执行，作业总是成功。
 */
@Component
public class DefaultJobExecutionHandler implements JobExecutionHandler {

    private static final Logger log = LoggerFactory.getLogger(DefaultJobExecutionHandler.class);

    @Override
    public boolean execute(String jobId) {
        log.info("executing job jobId={}", jobId);
        return true;
    }
}
