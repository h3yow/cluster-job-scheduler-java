package com.github.h3yow.clusterjobschedulerjava.tests.support;

import com.github.h3yow.clusterjobschedulerjava.config.SchedulerProperties;
import com.github.h3yow.clusterjobschedulerjava.model.Resources;
import com.github.h3yow.clusterjobschedulerjava.scheduler.JobExecutionHandler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试工具：可控执行器与轮询等待。
 */
public final class TestSupport {

    private TestSupport() {
    }

    /**
     * 记录每个作业的实际执行次数，确保同一作业不会被并发执行多次；
     * 支持“前 N 次失败”“总是失败”以及阻塞长任务。
     */
    public static class TrackingHandler implements JobExecutionHandler {
        public final Map<String, AtomicInteger> execCount = new ConcurrentHashMap<>();
        public final Set<String> concurrentOverlap = ConcurrentHashMap.newKeySet();
        public final Map<String, Integer> inflight = new ConcurrentHashMap<>();
        public final Set<String> failFirstN = ConcurrentHashMap.newKeySet();
        private final Map<String, Integer> failureTimes = new ConcurrentHashMap<>();
        public volatile boolean alwaysFail = false;
        public volatile CountDownLatch releaseGate = new CountDownLatch(0);
        private volatile boolean blocking = false;

        /** 打开阻塞模式：之后启动的作业会一直挂起，直到 releaseAll()。 */
        public void blockExecutions() {
            this.blocking = true;
            this.releaseGate = new CountDownLatch(1);
        }

        /** 放行所有被阻塞的（以及后续）执行。 */
        public void releaseAll() {
            this.blocking = false;
            this.releaseGate.countDown();
        }

        public void failFirstTimes(String jobId, int failures) {
            failureTimes.put(jobId, failures);
            failFirstN.add(jobId);
        }

        @Override
        public boolean execute(String jobId) {
            int running = inflight.merge(jobId, 1, Integer::sum);
            if (running > 1) {
                concurrentOverlap.add(jobId);
            }
            AtomicInteger counter = execCount.computeIfAbsent(jobId, k -> new AtomicInteger());
            int attempt = counter.incrementAndGet();
            try {
                if (blocking) {
                    releaseGate.await(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inflight.merge(jobId, -1, Integer::sum);
            }
            if (alwaysFail) {
                return false;
            }
            int failures = failureTimes.getOrDefault(jobId, 0);
            return attempt > failures;
        }

        public int countOf(String jobId) {
            return execCount.getOrDefault(jobId, new AtomicInteger(0)).get();
        }
    }

    public static SchedulerProperties props(String nodes, int maxAttempts, long backoffMs,
                                            long maxJobCpu, long maxJobMem,
                                            long tenantCpu, long tenantMem) {
        SchedulerProperties p = new SchedulerProperties();
        p.setNodes(nodes);
        p.setMaxAttempts(maxAttempts);
        p.setRetryBackoffMs(backoffMs);
        p.setMaxJobCpu(maxJobCpu);
        p.setMaxJobMemoryMb(maxJobMem);
        p.setTenantCpuQuota(tenantCpu);
        p.setTenantMemoryQuotaMb(tenantMem);
        return p;
    }

    public static Resources res(long cpu, long mem) {
        return new Resources(cpu, mem);
    }

    /** 轮询等待断言条件成立。 */
    public static void waitFor(long timeoutMs, Check check) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        AssertionError last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                check.run();
                return;
            } catch (AssertionError e) {
                last = e;
                Thread.sleep(10);
            }
        }
        if (last != null) {
            throw last;
        }
        throw new AssertionError("condition not met before timeout");
    }

    @FunctionalInterface
    public interface Check {
        void run() throws AssertionError;
    }
}
