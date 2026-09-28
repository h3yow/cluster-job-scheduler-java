package com.github.h3yow.clusterjobschedulerjava.tests.scheduler;

import com.github.h3yow.clusterjobschedulerjava.scheduler.cluster.Cluster;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.ClusterNode;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.Job;
import com.github.h3yow.clusterjobschedulerjava.scheduler.engine.SchedulerCore;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.CompletionOutcome;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobPriority;
import com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobState;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.RejectReason;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.SubmitResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.core;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.quotaManager;
import static com.github.h3yow.clusterjobschedulerjava.tests.scheduler.SchedulerTestFixtures.request;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并发场景：并发提交 / 配额竞争 / 提交与取消与失败回报交织。 */
class ConcurrentSchedulingTest {
    private static final Logger log = LoggerFactory.getLogger(ConcurrentSchedulingTest.class);

    @Test
    void concurrentSubmitsNeverOversellNode() throws Exception {
        // 单节点 4C；20 个线程同时提交 1C 作业：最多 4 个 RUNNING，其余 QUEUED。
        Cluster cluster = SchedulerTestFixtures.singleNodeCluster(4, 8_192L);
        SchedulerCore core = core(cluster, quotaManager("t", 100, 64, 131_072L), 1);

        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<SubmitResult> results = Collections.synchronizedList(new ArrayList<>());

        log.info("[concurrent-submit] input: threads={}, request=1C/1024MB, nodeCapacity=4C/8192MB", threads);
        for (int i = 0; i < threads; i++) {
            String id = "c" + i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                results.add(core.submit(request(id, "t", 1, 1_024L, JobPriority.NORMAL)));
            });
        }
        ready.await();
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long running = core.listJobs().stream().filter(j -> j.getState() == JobState.RUNNING).count();
        long queued = core.listJobs().stream().filter(j -> j.getState() == JobState.QUEUED).count();
        ClusterNode node = cluster.nodes().get(0);
        log.info("[concurrent-submit] result: accepted={}, running={}, queued={}, nodeUsedCpu={}, runningOnNode={}",
                results.stream().filter(SubmitResult::accepted).count(), running, queued,
                node.getUsed().getCpuCores(), node.getRunningJobIds().size());

        assertEquals(threads, results.stream().filter(SubmitResult::accepted).count());
        assertEquals(4, running, "容量为 4C，最多 4 个 1C 作业同时运行");
        assertEquals(threads - 4, queued);
        assertEquals(4, node.getUsed().getCpuCores(), "不得超卖");
        assertEquals(4, node.getRunningJobIds().size());
        assertNodeConsistency(core);
    }

    @Test
    void concurrentQuotaConflictGivesExactlyQuotaWinners() throws Exception {
        // 单节点 8C，租户配额 maxJobs=4、8C；30 个 1C 并发提交：恰好 4 个接受并运行，其余配额拒绝。
        Cluster cluster = SchedulerTestFixtures.singleNodeCluster(8, 16_384L);
        SchedulerCore core = core(cluster, quotaManager("t", 4, 8, 16_384L), 1);

        int threads = 30;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<SubmitResult> results = Collections.synchronizedList(new ArrayList<>());

        log.info("[concurrent-quota] input: threads={}, request=1C/1024MB, quota=maxJobs4/8C, node=8C", threads);
        for (int i = 0; i < threads; i++) {
            String id = "q" + i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                results.add(core.submit(request(id, "t", 1, 1_024L, JobPriority.NORMAL)));
            });
        }
        ready.await();
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long accepted = results.stream().filter(SubmitResult::accepted).count();
        long quotaRejected = results.stream()
                .filter(r -> !r.accepted() && r.rejectReason() == RejectReason.QUOTA_EXCEEDED).count();
        long duplicates = results.stream().filter(SubmitResult::duplicate).count();
        log.info("[concurrent-quota] output: accepted={}, quotaRejected={}, duplicates={}, nodeUsedCpu={}",
                accepted, quotaRejected, duplicates, cluster.nodes().get(0).getUsed().getCpuCores());

        assertEquals(4, accepted, "只有配额允许的 4 个作业能被接受");
        assertEquals(threads - 4, quotaRejected);
        assertEquals(0, duplicates, "每个作业只提交一次，绝不允许重复占位");
        assertEquals(4, cluster.nodes().get(0).getUsed().getCpuCores());
        assertNodeConsistency(core);
    }

    @Test
    void interleavedSubmitCancelAndFailureReportsStayConsistent() throws Exception {
        // 两节点共 16C；栅栏并发提交 40 个 1C 作业后，取消 / 成功 / 失败回报交织发生。
        Cluster cluster = SchedulerTestFixtures.twoNodeCluster();
        SchedulerCore core = core(cluster, quotaManager("t", 100, 64, 131_072L), 2);

        int threads = 40;
        ExecutorService submitters = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);

        log.info("[interleave] input: submitThreads={}, request=1C/1024MB, cluster=2x8C, maxAttempts=2", threads);
        for (int i = 0; i < threads; i++) {
            String id = "j" + i;
            submitters.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                core.submit(request(id, "t", 1, 1_024L, JobPriority.NORMAL));
            });
        }
        ready.await();
        start.countDown();
        submitters.shutdown();
        assertTrue(submitters.awaitTermination(10, TimeUnit.SECONDS));

        // 交织阶段：多线程分别取消、成功回报、失败回报；目标作业从实时快照取，允许 NOT_FOUND/NOT_RUNNING。
        ExecutorService actors = Executors.newFixedThreadPool(12);
        AtomicInteger cancelOk = new AtomicInteger();
        AtomicInteger successOk = new AtomicInteger();
        AtomicInteger failureOk = new AtomicInteger();
        for (int round = 0; round < 4; round++) {
            List<Job> snapshot = new ArrayList<>(core.listJobs());
            for (Job job : snapshot) {
                String id = job.getId();
                actors.submit(() -> {
                    if (core.cancel(id, "concurrent cancel round")) cancelOk.incrementAndGet();
                });
                actors.submit(() -> {
                    if (core.reportCompletion(id, CompletionOutcome.SUCCESS, "ok")) successOk.incrementAndGet();
                });
                actors.submit(() -> {
                    if (core.reportCompletion(id, CompletionOutcome.FAILURE, "simulated failure")) failureOk.incrementAndGet();
                });
            }
        }
        actors.shutdown();
        assertTrue(actors.awaitTermination(15, TimeUnit.SECONDS));
        core.schedulePending();

        List<Job> finalJobs = core.listJobs();
        long byStateQueued = finalJobs.stream().filter(j -> j.getState() == JobState.QUEUED).count();
        long byStateRunning = finalJobs.stream().filter(j -> j.getState() == JobState.RUNNING).count();
        long terminal = finalJobs.stream().filter(j -> j.getState().isTerminal()).count();
        log.info("[interleave] effects: cancelOk={}, successOk={}, failureOk={}",
                cancelOk.get(), successOk.get(), failureOk.get());
        log.info("[interleave] final states: queued={}, running={}, terminal={} (total={})",
                byStateQueued, byStateRunning, terminal, finalJobs.size());

        // 所有作业都有明确状态，且 queued+running+terminal == 总数
        assertEquals(threads, byStateQueued + byStateRunning + terminal);

        // 核心不变量：节点占用与 RUNNING 作业声明严格一致，无超卖、无泄漏、无重复占位
        assertNodeConsistency(core);
        for (ClusterNode node : cluster.nodes()) {
            assertTrue(node.getUsed().getCpuCores() <= node.getCapacity().getCpuCores());
        }
    }

    /** 校验：节点已占用资源 == 该节点上每个 RUNNING 作业资源之和；作业视图与节点视图互相吻合。 */
    private static void assertNodeConsistency(SchedulerCore core) {
        for (ClusterNode node : core.getCluster().nodes()) {
            int expectedCpu = 0;
            for (String jobId : node.getRunningJobIds()) {
                Job job = core.getJob(jobId).orElseThrow();
                assertEquals(JobState.RUNNING, job.getState(), "节点上的作业必须是 RUNNING");
                assertEquals(node.getId(), job.getAssignedNodeId());
                expectedCpu += job.getRequired().getCpuCores();
            }
            assertEquals(expectedCpu, node.getUsed().getCpuCores(),
                    "节点 " + node.getId() + " 占用与 RUNNING 作业资源之和不一致（泄漏/超卖/重复占位）");
        }
    }
}
