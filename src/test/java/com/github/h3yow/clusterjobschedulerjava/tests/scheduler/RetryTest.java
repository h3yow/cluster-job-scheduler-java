package com.github.h3yow.clusterjobschedulerjava.tests.scheduler;

import com.github.h3yow.clusterjobschedulerjava.scheduler.cluster.Cluster;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.Job;
import com.github.h3yow.clusterjobschedulerjava.scheduler.engine.SchedulerCore;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.CompletionOutcome;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobPriority;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobState;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.core;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.quotaManager;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.requestWithAttempts;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.singleNodeCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 失败重试：重新排队、资源归还、重试上限进入 FAILED 终态、终态回报被忽略。 */
class RetryTest {
    private static final Logger log = LoggerFactory.getLogger(RetryTest.class);

    @Test
    void failureIncrementsAttemptReleasesAndImmediatelyReassignsWhenCapacityFree() {
        // 单节点 10C，flaky 需要 8C：队列中没有其他作业时，失败释放后会在同一轮立即重新分派。
        Cluster cluster = singleNodeCluster(10, 20_480L);
        SchedulerCore core = core(cluster, quotaManager("t", 10, 64, 131_072L), 3);

        core.submit(requestWithAttempts("flaky", "t", 8, 16_000L, JobPriority.NORMAL, 3));
        assertEquals(JobState.RUNNING, core.getJob("flaky").orElseThrow().getState());

        // 第 1 次失败：attempt 增加为 2，释放后容量空闲，同轮立即重新占位成功。
        assertTrue(core.reportCompletion("flaky", CompletionOutcome.FAILURE, "transient io error"));
        Job afterFail = core.getJob("flaky").orElseThrow();
        log.info("[retry] after failure#1 state={} attempt={} node={}",
                afterFail.getState(), afterFail.getAttempt(), afterFail.getAssignedNodeId());
        assertEquals(JobState.RUNNING, afterFail.getState());
        assertEquals(2, afterFail.getAttempt());
        assertEquals(8, cluster.nodes().get(0).getUsed().getCpuCores());

        // 最终成功：进入 SUCCEEDED 终态，资源归零
        core.reportCompletion("flaky", CompletionOutcome.SUCCESS, "ok");
        assertEquals(JobState.SUCCEEDED, core.getJob("flaky").orElseThrow().getState());
        assertEquals(0, cluster.nodes().get(0).getUsed().getCpuCores());
    }

    @Test
    void failedJobWaitsQueuedWhenCapacityHeldThenRedispatches() {
        // 节点 10C：victim 8C 与 blocker 3C 无法共存。
        Cluster cluster = singleNodeCluster(10, 20_480L);
        SchedulerCore core = core(cluster, quotaManager("t", 10, 64, 131_072L), 3);

        // blocker 先占 3C；victim(8C) 此时放不下，保持 QUEUED。
        core.submit(requestWithAttempts("blocker", "t", 3, 3_000L, JobPriority.HIGH, 1));
        core.submit(requestWithAttempts("victim", "t", 8, 16_000L, JobPriority.NORMAL, 3));
        assertEquals(JobState.RUNNING, core.getJob("blocker").orElseThrow().getState());
        Job victim = core.getJob("victim").orElseThrow();
        assertEquals(JobState.QUEUED, victim.getState());
        assertNotNull(victim.getLastScheduleSkip());

        // blocker 释放后，victim 被分派（attempt=1）
        core.reportCompletion("blocker", CompletionOutcome.SUCCESS, "");
        core.schedulePending();
        assertEquals(JobState.RUNNING, core.getJob("victim").orElseThrow().getState());

        // victim 第一次失败；先让高优先级 hold(3C) 在队列里等着，
        // victim 失败释放后 hold 立刻占位，victim(8C) 确定性地保持 QUEUED。
        core.submit(requestWithAttempts("hold", "t", 3, 3_000L, JobPriority.HIGH, 1));
        assertEquals(JobState.QUEUED, core.getJob("hold").orElseThrow().getState());

        core.reportCompletion("victim", CompletionOutcome.FAILURE, "boom");
        Job afterFail = core.getJob("victim").orElseThrow();
        Job hold = core.getJob("hold").orElseThrow();
        log.info("[retry-queue] victim state={} attempt={} node={} skip={} | hold state={} node={} nodeUsedCpu={}",
                afterFail.getState(), afterFail.getAttempt(), afterFail.getAssignedNodeId(),
                afterFail.getLastScheduleSkip(), hold.getState(), hold.getAssignedNodeId(),
                cluster.nodes().get(0).getUsed().getCpuCores());

        assertEquals(JobState.QUEUED, afterFail.getState(), "容量被 hold 占据，victim 必须回到排队态");
        assertEquals(2, afterFail.getAttempt());
        assertEquals(JobState.RUNNING, hold.getState());
        assertNotNull(afterFail.getLastScheduleSkip());
        assertEquals(3, cluster.nodes().get(0).getUsed().getCpuCores(), "victim 的 8C 必须已完整释放");

        // hold 完成，victim 再次分派（attempt=2），之后成功
        core.reportCompletion("hold", CompletionOutcome.SUCCESS, "");
        core.schedulePending();
        assertEquals(JobState.RUNNING, core.getJob("victim").orElseThrow().getState());
        assertEquals(2, core.getJob("victim").orElseThrow().getAttempt());
        core.reportCompletion("victim", CompletionOutcome.SUCCESS, "done");
        assertEquals(JobState.SUCCEEDED, core.getJob("victim").orElseThrow().getState());
        assertEquals(0, cluster.nodes().get(0).getUsed().getCpuCores());
    }

    @Test
    void failsAfterMaxAttemptsAndTerminalStateIsStable() {
        // 用 hold 占容量，使 doomed 在每次失败后保持 QUEUED，从而确定性地打满尝试次数。
        Cluster cluster = singleNodeCluster(10, 20_480L);
        SchedulerCore core = core(cluster, quotaManager("t", 10, 64, 131_072L), 2);

        core.submit(requestWithAttempts("hold", "t", 3, 3_000L, JobPriority.HIGH, 1));
        core.submit(requestWithAttempts("doomed", "t", 8, 16_000L, JobPriority.NORMAL, 2));
        core.reportCompletion("hold", CompletionOutcome.SUCCESS, "");
        core.schedulePending();
        assertEquals(JobState.RUNNING, core.getJob("doomed").orElseThrow().getState());

        // 第 1 次失败：attempt 2 立即重新分派
        core.reportCompletion("doomed", CompletionOutcome.FAILURE, "err-1");
        Job mid = core.getJob("doomed").orElseThrow();
        log.info("[retry-limit] after failure#1 state={} attempt={}", mid.getState(), mid.getAttempt());
        assertEquals(2, mid.getAttempt());
        assertEquals(JobState.RUNNING, mid.getState());

        // 第 2 次失败：达到上限 -> FAILED 终态
        core.reportCompletion("doomed", CompletionOutcome.FAILURE, "err-2");
        Job job = core.getJob("doomed").orElseThrow();
        log.info("[retry-limit] final state={} attempts={} terminalReason={}",
                job.getState(), job.getAttempt(), job.getTerminalReason());
        assertEquals(JobState.FAILED, job.getState());
        assertTrue(job.getState().isTerminal());
        assertTrue(job.getTerminalReason().contains("max attempts"));
        assertEquals(0, cluster.nodes().get(0).getUsed().getCpuCores());

        // 终态后迟到的回报必须被忽略，不能覆盖终态
        boolean lateReport = core.reportCompletion("doomed", CompletionOutcome.SUCCESS, "late");
        log.info("[retry-limit] late report accepted={} state stays={}", lateReport,
                core.getJob("doomed").orElseThrow().getState());
        assertFalse(lateReport);
        assertEquals(JobState.FAILED, core.getJob("doomed").orElseThrow().getState());
        assertFalse(core.cancel("doomed", "too late"));
    }
}
