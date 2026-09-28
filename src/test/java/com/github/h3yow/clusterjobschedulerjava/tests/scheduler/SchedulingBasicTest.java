package com.github.h3yow.clusterjobschedulerjava.tests.scheduler;

import com.github.h3yow.clusterjobschedulerjava.scheduler.cluster.Cluster;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.ClusterNode;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.Job;
import com.github.h3yow.clusterjobschedulerjava.scheduler.engine.SchedulerCore;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobPriority;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobState;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.RejectReason;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.SubmitResult;
import com.github.h3yow.clusterjobschedulerjava.scheduler.quota.QuotaManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.core;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.quotaManager;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.request;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.twoNodeCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 基础分派 / 优先级 / 拒绝原因 / 幂等。 */
class SchedulingBasicTest {
    private static final Logger log = LoggerFactory.getLogger(SchedulingBasicTest.class);

    @Test
    void submitsGetScheduledAndResourcesAccounted() {
        Cluster cluster = twoNodeCluster();
        SchedulerCore core = core(cluster, quotaManager("team-a", 10, 16, 32_768L), 3);

        SubmitResult r1 = core.submit(request("j1", "team-a", 6, 8_192L, JobPriority.NORMAL));
        SubmitResult r2 = core.submit(request("j2", "team-a", 6, 8_192L, JobPriority.NORMAL));
        log.info("[basic] j1 => accepted={}, state={}, node={}", r1.accepted(), r1.state(),
                core.getJob("j1").map(Job::getAssignedNodeId).orElse(null));
        log.info("[basic] j2 => accepted={}, state={}, node={}", r2.accepted(), r2.state(),
                core.getJob("j2").map(Job::getAssignedNodeId).orElse(null));

        assertTrue(r1.accepted());
        assertTrue(r2.accepted());
        assertEquals(JobState.RUNNING, core.getJob("j1").orElseThrow().getState());
        assertEquals(JobState.RUNNING, core.getJob("j2").orElseThrow().getState());

        int totalUsedCpu = cluster.nodes().stream().mapToInt(n -> n.getUsed().getCpuCores()).sum();
        long totalUsedMem = cluster.nodes().stream().mapToLong(n -> n.getUsed().getMemoryMb()).sum();
        log.info("[basic] cluster used after 2 jobs: cpu={}, memoryMb={}", totalUsedCpu, totalUsedMem);
        assertEquals(12, totalUsedCpu);
        assertEquals(16_384L, totalUsedMem);
    }

    @Test
    void highPriorityDispatchesFirstAndExplainsPendingSkip() throws Exception {
        // 单节点 10C：先占一个 8C 的大作业，使剩余 2C。
        Cluster cluster = SchedulerTestFixtures.singleNodeCluster(10, 20_480L);
        SchedulerCore core = core(cluster, quotaManager("t", 100, 64, 131_072L), 3);

        core.submit(request("big", "t", 8, 8_192L, JobPriority.NORMAL));

        // 用两个线程保证 low 先入队、high 后入队，验证优先级插队。
        Thread lowFirst = new Thread(() ->
                core.submit(request("low", "t", 4, 4_096L, JobPriority.LOW)));
        lowFirst.start();
        lowFirst.join();
        core.submit(request("high", "t", 1, 1_024L, JobPriority.HIGH));

        Job low = core.getJob("low").orElseThrow();
        Job high = core.getJob("high").orElseThrow();
        log.info("[priority] high state={} node={} | low state={} node={} skip={}",
                high.getState(), high.getAssignedNodeId(), low.getState(), low.getAssignedNodeId(),
                low.getLastScheduleSkip());

        assertEquals(JobState.RUNNING, high.getState());
        assertEquals(JobState.QUEUED, low.getState());
        assertNotNull(low.getLastScheduleSkip(), "未调度作业必须能查到具体原因");
        assertTrue(low.getEvents().stream().anyMatch(e -> e.type().equals("SCHEDULE_SKIPPED")));

        // big 完成后，下一轮调度 low 才下发
        core.reportCompletion("big", com.github.h3yow.clusterjobschedulerjava.scheduler.model.CompletionOutcome.SUCCESS, "");
        core.schedulePending();
        assertEquals(JobState.RUNNING, core.getJob("low").orElseThrow().getState());
    }

    @Test
    void duplicateSubmissionIsIdempotent() {
        SchedulerCore core = core(twoNodeCluster(), quotaManager("t", 10, 16, 32_768L), 3);
        var req = request("dup", "t", 2, 2_048L, JobPriority.NORMAL);

        SubmitResult first = core.submit(req);
        SubmitResult second = core.submit(req);
        log.info("[idempotent] first accepted={} | second duplicate={} state={}",
                first.accepted(), second.duplicate(), second.state());

        assertTrue(first.accepted());
        assertTrue(second.duplicate());
        assertFalse(second.accepted());
        long runningCount = core.listJobs().stream().filter(j -> j.getId().equals("dup")).count();
        assertEquals(1, runningCount, "重复提交不得产生多条作业/多次执行");
    }

    @Test
    void rejectsInvalidArgument() {
        SchedulerCore core = core(twoNodeCluster(), quotaManager("t", 10, 16, 32_768L), 3);
        SubmitResult r = core.submit(request("bad", "t", 0, 0L, JobPriority.NORMAL));
        log.info("[reject] invalid => accepted={} reason={} message={}", r.accepted(), r.rejectReason(), r.message());
        assertFalse(r.accepted());
        assertEquals(RejectReason.INVALID_ARGUMENT, r.rejectReason());
    }

    @Test
    void rejectsJobExceedingPhysicalCapacity() {
        SchedulerCore core = core(twoNodeCluster(), quotaManager("t", 100, 10_000, 1_000_000L), 3);
        SubmitResult r = core.submit(request("huge", "t", 100, 100_000L, JobPriority.NORMAL));
        log.info("[reject] capacity => accepted={} reason={} message={}", r.accepted(), r.rejectReason(), r.message());
        assertFalse(r.accepted());
        assertEquals(RejectReason.INSUFFICIENT_CAPACITY, r.rejectReason());

        Job stored = core.getJob("huge").orElseThrow();
        assertEquals(JobState.REJECTED, stored.getState());
        assertEquals(RejectReason.INSUFFICIENT_CAPACITY, stored.getRejectReason());
        assertNotNull(stored.getTerminalReason());
    }
}
