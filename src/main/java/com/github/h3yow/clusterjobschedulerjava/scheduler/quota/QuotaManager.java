package com.github.h3yow.clusterjobschedulerjava.scheduler.quota;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/** 租户配额注册表。 */
public final class QuotaManager {
    private final Map<String, QuotaRecord> quotas;
    private final QuotaRecord defaultQuota;

    public QuotaManager(Map<String, QuotaRecord> quotas, QuotaRecord defaultQuota) {
        this.quotas = Map.copyOf(quotas);
        this.defaultQuota = defaultQuota;
    }

    /** 返回租户显式配置的配额；未配置时为空。 */
    public Optional<QuotaRecord> configuredQuota(String tenant) {
        return Optional.ofNullable(quotas.get(tenant));
    }

    /** 生效配额：优先显式配置，否则回落默认配额。 */
    public QuotaRecord quotaOf(String tenant) {
        return quotas.getOrDefault(tenant, defaultQuota);
    }

    public Map<String, QuotaRecord> allQuotas() {
        return Collections.unmodifiableMap(quotas);
    }
}
