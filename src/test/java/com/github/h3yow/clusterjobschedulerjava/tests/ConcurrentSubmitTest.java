package com.github.h3yow.clusterjobschedulerjava.tests;

import com.github.h3yow.clusterjobschedulerjava.model.Job;
import com.github.h3yow.clusterjobschedulerjava.model.JobState;
import com.github.h3yow.clusterjobschedulerjava.model.RejectReason;
import com.github.h3yow.clusterjobschedulerjava.scheduler.JobScheduler;
import com.github.h3yow.clusterjobschedulerjava.scheduler.SubmitResult;
import com.github.h3yow.clusterjobschedulerjava.tests.support.TestSupport;
import com.github.h3yow.clusterjobschedulerjava.tests.support.TestSupport.TrackingHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发场景：大量并发提交不得超卖；同 id 并发提交只有一次执行；高优先级先分派；取消与完成竞争安全。
 */
class ConcurrentSubmitTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentSubmitTest.class);

    private JobScheduler scheduler;
    private TrackingHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TrackingHandler();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    @DisplayName("并发提交 40 个 3C 作业到 2x8C 集群：任意时刻已用 CPU 不超过容量 16")
    void concurrentSubmissionsNeverOversell() throws Exception {
        // 单作业 3C，租户配额关闭，2 节点共 16C；作业阻塞以便堆积 PENDING
        var props = TestSupport.props("n1:8:16384,n2:8:16384", 1, 5,
                8, 16384, 0, 0);
        scheduler = new JobScheduler(props, handler);
        scheduler.initNodes();

        int threads = 40;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            final String id = "j-" + i;
            final String tenant = "t" + (i % 4);
            pool.submit(() -> {
                try {
                    start.await();
                    SubmitResult r = scheduler.submit(id, tenant, 0, TestSupport.res(3, 256));
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                    } else {
                        rejected.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        handler.blockExecutions();
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        log.info("[input] concurrent submissions={}, accepted={}, rejected={}",
                threads, accepted.get(), rejected.get());

        // 并发下不断巡检：节点已分配绝不能超过物理容量（不超卖）
        long peak = 0;
        for (int i = 0; i < 50; i++) {
            long used = scheduler.listNodes().stream()
                    .mapToLong(n -> n.getUsed().cpu()).sum();
            peak = Math.max(peak, used);
            assertTrue(used <= 16L, "oversold! used cpu=" + used);
            // 每个节点也不得超卖
            scheduler.listNodes().forEach(n -> assertTrue(n.getUsed().cpu() <= n.getCapacity().cpu()));
            Thread.sleep(2);
        }
        log.info("[assert] peak used cpu while jobs held = {}", peak);

        // 放行全部执行
        handler.releaseAll();
        TestSupport.waitFor(10000, () -> {
            long runningOrPending = scheduler.listJobs().stream()
                    .filter(j -> j.getState() == JobState.RUNNING || j.getState() == JobState.PENDING)
                    .count();
            assertEquals(0L, runningOrPending);
        });

        long terminalCpu = scheduler.listNodes().stream()
                .mapToLong(n -> n.getUsed().cpu()).sum();
        long succeeded = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.SUCCEEDED).count();
        log.info("[assert] succeeded={}, final used cpu={}", succeeded, terminalCpu);
        assertEquals(40L, succeeded);
        assertEquals(0L, terminalCpu, "all resources released after terminal");
    }

    @Test
    @DisplayName("同一 id 被 20 个线程同时提交：恰好一次接受、恰好一次执行")
    void sameIdConcurrentSubmitExecutesOnce() throws Exception {
        var props = TestSupport.props("n1:8:16384", 1, 5, 8, 16384, 0, 0);
        scheduler = new JobScheduler(props, handler);
        scheduler.initNodes();

        int threads = 20;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicateRejects = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    SubmitResult r = scheduler.submit("only", "t1", 0, TestSupport.res(2, 256));
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                    } else if (r.rejectReason() == RejectReason.DUPLICATE_JOB) {
                        duplicateRejects.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        handler.blockExecutions();
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        handler.releaseAll();

        TestSupport.waitFor(5000, () ->
                assertEquals(JobState.SUCCEEDED, scheduler.getJob("only").getState()));
        log.info("[assert] accepted={}, duplicateRejects={}, executions={}",
                accepted.get(), duplicateRejects.get(), handler.countOf("only"));
        assertEquals(1, accepted.get());
        assertEquals(19, duplicateRejects.get());
        assertEquals(1, handler.countOf("only"));
        assertTrue(handler.concurrentOverlap.isEmpty());
    }

    @Test
    @DisplayName("资源紧张时高优先级作业先分派")
    void higherPriorityDispatchedFirst() throws Exception {
        var props = TestSupport.props("n1:8:16384", 1, 5, 8, 16384, 0, 0);
        scheduler = new JobScheduler(props, handler);
        scheduler.initNodes();

        // 8C 作业占满唯一节点并阻塞
        handler.blockExecutions();
        scheduler.submit("holder", "t1", 0, TestSupport.res(8, 256));
        assertEquals(JobState.RUNNING, scheduler.getJob("holder").getState());

        // 低优先级先提交、高优先级后提交，二者都排队（都申请 6C，同一时刻只能跑一个）
        SubmitResult low = scheduler.submit("low", "t2", 1, TestSupport.res(6, 256));
        SubmitResult high = scheduler.submit("high", "t3", 10, TestSupport.res(6, 256));
        log.info("[input] holder running(8C); low(prio=1) accepted={}, high(prio=10) accepted={}",
                low.accepted(), high.accepted());
        assertEquals(JobState.PENDING, scheduler.getJob("low").getState());
        assertEquals(JobState.PENDING, scheduler.getJob("high").getState());

        // 释放 holder：下一轮分派必须先选 high
        log.info("[input] release holder, expect high dispatched before low");
        handler.releaseAll();
        TestSupport.waitFor(5000, () ->
                assertEquals(JobState.SUCCEEDED, scheduler.getJob("high").getState()));

        TestSupport.waitFor(5000, () ->
                assertEquals(JobState.SUCCEEDED, scheduler.getJob("low").getState()));

        // 权威断言：调度器锁内记录的分派/终态顺序中 high 必须在 low 之前
        List<String> order = scheduler.terminalOrder();
        log.info("[assert] terminal order = {}", order);
        assertTrue(order.indexOf("high") < order.indexOf("low"),
                "high priority job must reach terminal state before low priority job");
    }

    @Test
    @DisplayName("取消 PENDING 作业：释放队列占位，终态 CANCELLED，不再执行")
    void cancelPendingJob() throws Exception {
        var props = TestSupport.props("n1:8:16384", 1, 5, 8, 16384, 0, 0);
        scheduler = new JobScheduler(props, handler);
        scheduler.initNodes();

        // 先让 1C 作业占住节点（阻塞），再提交 8C 作业，节点仅剩 7C，必然排队
        handler.blockExecutions();
        scheduler.submit("hold", "t1", 0, TestSupport.res(1, 256));
        assertEquals(JobState.RUNNING, scheduler.getJob("hold").getState());
        scheduler.submit("wait", "t2", 0, TestSupport.res(8, 256));
        assertEquals(JobState.PENDING, scheduler.getJob("wait").getState());

        Job cancelled = scheduler.cancel("wait");
        assertEquals(JobState.CANCELLED, cancelled.getState());
        log.info("[decision] wait cancelled from PENDING, executions={}", handler.countOf("wait"));
        assertEquals(0, handler.countOf("wait"));

        handler.releaseAll();
        TestSupport.waitFor(5000, () ->
                assertEquals(JobState.SUCCEEDED, scheduler.getJob("hold").getState()));
        // wait 保持终态
        assertEquals(JobState.CANCELLED, scheduler.getJob("wait").getState());
        assertEquals(0, handler.countOf("wait"));
    }
}
