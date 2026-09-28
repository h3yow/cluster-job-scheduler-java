package com.github.h3yow.clusterjobschedulerjava.scheduler.quota;

import com.github.h3yow.clusterjobschedulerjava.scheduler.model.Resources;

/** 单个租户的配额：可并发保活作业数与资源总量上限。 */
public record QuotaRecord(int maxJobs, Resources maxResources) {
}
