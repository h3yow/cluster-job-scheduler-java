package com.github.h3yow.clusterjobschedulerjava.tests.scheduler;

import com.github.h3yow.clusterjobschedulerjava.scheduler.cluster.Cluster;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.Job;
import com.github.h3yow.clusterjobschedulerjava.scheduler.engine.SchedulerCore;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobPriority;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobState;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.RejectReason;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.SubmitResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.core;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.quotaManager;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.request;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.twoNodeCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 配额冲突：作业数上限与资源上限，以及终态释放配额后的再次提交。 */
class QuotaConflictTest {
    private static final Logger log = LoggerFactory.getLogger(QuotaConflictTest.class);

    @Test
    void rejectsWhenMaxJobsQuotaExceeded() {
        // team-a：最多 2 个并发保活作业；集群资源足够，确保拒绝原因纯粹是配额。
        Cluster cluster = twoNodeCluster();
        SchedulerCore core = core(cluster, quotaManager("team-a", 2, 16, 32_768L), 3);

        SubmitResult q1 = core.submit(request("q1", "team-a", 1, 1_024L, JobPriority.NORMAL));
        SubmitResult q2 = core.submit(request("q2", "team-a", 1, 1_024L, JobPriority.NORMAL));
        SubmitResult q3 = core.submit(request("q3", "team-a", 1, 1_024L, JobPriority.NORMAL));

        log.info("[quota] q1.accepted={} q2.accepted={} q3.accepted={} q3.reason={} q3.message={}",
                q1.accepted(), q2.accepted(), q3.accepted(), q3.rejectReason(), q3.message());

        assertTrue(q1.accepted());
        assertTrue(q2.accepted());
        assertFalse(q3.accepted());
        assertEquals(RejectReason.QUOTA_EXCEEDED, q3.rejectReason());
        assertEquals(JobState.REJECTED, core.getJob("q3").orElseThrow().getState());

        // q1 终态后释放出 1 个作业位，q4 应被接受
        core.reportCompletion("q1",
                com.github.h3yow.clusterjobschedulerjava.scheduler.model.CompletionOutcome.SUCCESS, "");
        SubmitResult q4 = core.submit(request("q4", "team-a", 1, 1_024L, JobPriority.NORMAL));
        log.info("[quota] after q1 finished, q4.accepted={} state={}", q4.accepted(), q4.state());
        assertTrue(q4.accepted());

        long active = core.listJobs().stream()
                .filter(j -> j.getTenant().equals("team-a"))
                .filter(j -> j.getState() == JobState.QUEUED || j.getState() == JobState.RUNNING)
                .count();
        log.info("[quota] active jobs of team-a finally = {}", active);
        assertEquals(2, active);
    }

    @Test
    void rejectsWhenResourceQuotaExceeded() {
        // 作业数给足（10），但资源配额只有 6C / 12GB；集群容量 16C/32GB。
        SchedulerCore core = core(twoNodeCluster(), quotaManager("team-a", 10, 6, 12_288L), 3);

        SubmitResult a = core.submit(request("a", "team-a", 4, 8_192L, JobPriority.NORMAL));
        SubmitResult b = core.submit(request("b", "team-a", 4, 8_192L, JobPriority.NORMAL));

        log.info("[quota-res] a.accepted={} state={} | b.accepted={} reason={} message={}",
                a.accepted(), a.state(), b.accepted(), b.rejectReason(), b.message());

        assertTrue(a.accepted());
        assertFalse(b.accepted());
        assertEquals(RejectReason.QUOTA_EXCEEDED, b.rejectReason());
        Job rejected = core.getJob("b").orElseThrow();
        assertEquals(JobState.REJECTED, rejected.getState());
        assertTrue(rejected.getTerminalReason().contains("quota"));
    }
}
