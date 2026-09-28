package com.github.h3yow.clusterjobschedulerjava.tests;

import com.github.h3yow.clusterjobschedulerjava.model.Job;
import com.github.h3yow.clusterjobschedulerjava.model.JobState;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败重试：达到上限进入 FAILED 终态；中途成功进入 SUCCEEDED；
 * 重试期间资源已释放；同一作业任一时刻至多一个执行。
 */
class RetryTest {

    private static final Logger log = LoggerFactory.getLogger(RetryTest.class);

    private JobScheduler scheduler;
    private TrackingHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TrackingHandler();
        // 单节点 8C，最多尝试 3 次，退避 30ms
        var props = TestSupport.props("n1:8:16384", 3, 30, 8, 16384, 0, 0);
        scheduler = new JobScheduler(props, handler);
        scheduler.initNodes();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    @DisplayName("总是失败的作业执行恰好 maxAttempts=3 次后进入 FAILED 终态")
    void failAfterMaxAttempts() throws Exception {
        handler.alwaysFail = true;
        log.info("[input] submit job=doomed, alwaysFail=true, maxAttempts=3, backoff=30ms");
        SubmitResult r = scheduler.submit("doomed", "t1", 0, TestSupport.res(2, 256));
        assertTrue(r.accepted());

        TestSupport.waitFor(10000, () ->
                assertEquals(JobState.FAILED, scheduler.getJob("doomed").getState()));

        Job job = scheduler.getJob("doomed");
        log.info("[assert] state={} attempts={} executions={} events={}",
                job.getState(), job.getAttempts(), handler.countOf("doomed"),
                job.getEvents().stream().map(e -> e.type()).toList());
        assertEquals(JobState.FAILED, job.getState());
        assertEquals(3, job.getAttempts());
        assertEquals(3, handler.countOf("doomed"));
        assertNotNullFinished(job);
        // 终态后资源全部归还
        assertEquals(0L, scheduler.listNodes().get(0).getUsed().cpu());
        // 审计轨迹包含两次失败重试和一次终态失败
        long attemptFailed = job.getEvents().stream()
                .filter(e -> e.type().equals("ATTEMPT_FAILED")).count();
        long terminalFailed = job.getEvents().stream()
                .filter(e -> e.type().equals("FAILED")).count();
        assertEquals(2L, attemptFailed, "two retriable failures");
        assertEquals(1L, terminalFailed, "one terminal failure event");
        assertTrue(handler.concurrentOverlap.isEmpty());
    }

    @Test
    @DisplayName("前两次失败第三次成功 -> SUCCEEDED，attempts=3，重试期间资源释放可被其他作业使用")
    void retryThenSucceedFreesResourcesBetweenAttempts() throws Exception {
        handler.failFirstTimes("flaky", 2);
        log.info("[input] submit job=flaky (fail first 2 attempts, then succeed)");
        scheduler.submit("flaky", "t1", 0, TestSupport.res(8, 256)); // 占满整个节点

        // flaky 第一次失败后回到 PENDING 且进入退避；此时节点应为空，8C 的 filler 能插入执行
        TestSupport.waitFor(10000, () ->
                assertEquals(JobState.PENDING, scheduler.getJob("flaky").getState()));
        assertEquals(0L, scheduler.listNodes().get(0).getUsed().cpu(),
                "resources must be released during retry backoff");
        log.info("[decision] flaky back in PENDING, node free cpu=0, inserting filler(8C)");

        // 用一个总是成功的处理器行为提交 filler：flaky 之外的作业默认成功
        SubmitResult filler = scheduler.submit("filler", "t2", 0, TestSupport.res(8, 256));
        assertTrue(filler.accepted());
        TestSupport.waitFor(10000, () ->
                assertEquals(JobState.SUCCEEDED, scheduler.getJob("filler").getState()));
        log.info("[assert] filler succeeded while flaky waiting for retry");

        TestSupport.waitFor(10000, () ->
                assertEquals(JobState.SUCCEEDED, scheduler.getJob("flaky").getState()));
        Job flaky = scheduler.getJob("flaky");
        log.info("[assert] flaky state={} attempts={} executions={}",
                flaky.getState(), flaky.getAttempts(), handler.countOf("flaky"));
        assertEquals(JobState.SUCCEEDED, flaky.getState());
        assertEquals(3, flaky.getAttempts());
        assertEquals(3, handler.countOf("flaky"));
        assertEquals(0L, scheduler.listNodes().get(0).getUsed().cpu());
    }

    @Test
    @DisplayName("重试退避生效：第二次尝试不会在退避窗口内立即启动")
    void backoffDelaysRetry() throws Exception {
        // backoff=300ms，总是失败；第二次执行的启动时间距首次提交至少有一个退避间隔
        var props = TestSupport.props("n1:8:16384", 3, 300, 8, 16384, 0, 0);
        scheduler.shutdown();
        TrackingHandler h2 = new TrackingHandler();
        h2.alwaysFail = true;
        scheduler = new JobScheduler(props, h2);
        scheduler.initNodes();

        long start = System.currentTimeMillis();
        scheduler.submit("slowretry", "t1", 0, TestSupport.res(2, 256));
        TestSupport.waitFor(10000, () -> {
            if (h2.countOf("slowretry") < 2) {
                throw new AssertionError("second attempt not started");
            }
        });
        long secondAttemptAt = System.currentTimeMillis();
        long elapsed = secondAttemptAt - start;
        log.info("[assert] time until second attempt started = {}ms (backoff=300ms)", elapsed);
        assertTrue(elapsed >= 250, "retry must respect backoff, elapsed=" + elapsed);
        TestSupport.waitFor(10000, () ->
                assertEquals(JobState.FAILED, scheduler.getJob("slowretry").getState()));
    }

    @Test
    @DisplayName("FAILED 终态作业取消无效、再次提交按重复处理")
    void terminalJobLifecycleStable() throws Exception {
        handler.alwaysFail = true;
        scheduler.submit("dead", "t1", 0, TestSupport.res(1, 64));
        TestSupport.waitFor(10000, () ->
                assertEquals(JobState.FAILED, scheduler.getJob("dead").getState()));

        log.info("[input] cancel terminal job=dead then resubmit same id");
        Job cancelled = scheduler.cancel("dead");
        assertEquals(null, cancelled, "cancel on terminal job must return null");
        assertEquals(JobState.FAILED, scheduler.getJob("dead").getState());

        SubmitResult dup = scheduler.submit("dead", "t1", 0, TestSupport.res(1, 64));
        assertFalse(dup.accepted());
        log.info("[assert] resubmit rejected reason={}, state stays FAILED, executions={}",
                dup.rejectReason(), handler.countOf("dead"));
        assertEquals(3, handler.countOf("dead"));
    }

    private static void assertNotNullFinished(Job job) {
        assertTrue(job.getFinishedAt() != null, "terminal job must have finishedAt");
    }
}
