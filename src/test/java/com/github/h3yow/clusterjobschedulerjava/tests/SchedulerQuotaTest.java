package com.github.h3yow.clusterjobschedulerjava.tests;

import com.github.h3yow.clusterjobschedulerjava.model.JobState;
import com.github.h3yow.clusterjobschedulerjava.model.NodeState;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 配额与拒绝原因：单作业配额、物理容量、租户配额、重复提交、资源暂时不足排队。
 */
class SchedulerQuotaTest {

    private static final Logger log = LoggerFactory.getLogger(SchedulerQuotaTest.class);

    private JobScheduler scheduler;
    private TrackingHandler handler;

    // 2 个 8C16G 节点；单作业上限 8C16G；租户配额 8C16G；最多 1 次尝试
    @BeforeEach
    void setUp() {
        handler = new TrackingHandler();
        // 默认让作业执行阻塞，测试可精确控制资源占用
        var props = TestSupport.props("n1:8:16384,n2:8:16384", 1, 10,
                8, 16384, 8, 16384);
        scheduler = new JobScheduler(props, handler);
        scheduler.initNodes();
        handler.blockExecutions();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    @DisplayName("超过单作业资源上限 -> JOB_QUOTA_EXCEEDED，且作业为 REJECTED 终态")
    void rejectWhenExceedPerJobQuota() {
        log.info("[input] submit job=big cpu=9 mem=1024, per-job quota cpu=8");
        SubmitResult r = scheduler.submit("big", "t1", 0, TestSupport.res(9, 1024));
        log.info("[decision] accepted={} reason={} message={}", r.accepted(), r.rejectReason(), r.message());

        assertFalse(r.accepted());
        assertEquals(RejectReason.JOB_QUOTA_EXCEEDED, r.rejectReason());
        assertEquals(JobState.REJECTED, scheduler.getJob("big").getState());
        assertEquals(RejectReason.JOB_QUOTA_EXCEEDED, scheduler.getJob("big").getRejectReason());
        assertNotNull(scheduler.getJob("big").getEvents().get(0).detail());
    }

    @Test
    @DisplayName("内存超过单作业上限 -> 同样 JOB_QUOTA_EXCEEDED（两维独立校验）")
    void rejectWhenExceedPerJobMemoryQuota() {
        log.info("[input] submit job=bigmem cpu=1 mem=20000, per-job quota mem=16384");
        SubmitResult r = scheduler.submit("bigmem", "t1", 0, TestSupport.res(1, 20000));
        log.info("[decision] accepted={} reason={}", r.accepted(), r.rejectReason());

        assertFalse(r.accepted());
        assertEquals(RejectReason.JOB_QUOTA_EXCEEDED, r.rejectReason());
    }

    @Test
    @DisplayName("申请超过集群最大节点容量 -> EXCEEDS_MAX_CAPACITY")
    void rejectWhenExceedPhysicalCapacity() {
        // 单作业配额放宽到 32C，但物理节点只有 8C
        var props = TestSupport.props("n1:8:16384,n2:8:16384", 1, 10,
                32, 65536, 0, 0);
        scheduler.shutdown();
        scheduler = new JobScheduler(props, handler);
        scheduler.initNodes();

        log.info("[input] submit job=huge cpu=16 mem=1024, largest node cpu=8");
        SubmitResult r = scheduler.submit("huge", "t1", 0, TestSupport.res(16, 1024));
        log.info("[decision] accepted={} reason={} message={}", r.accepted(), r.rejectReason(), r.message());

        assertFalse(r.accepted());
        assertEquals(RejectReason.EXCEEDS_MAX_CAPACITY, r.rejectReason());
        assertEquals(JobState.REJECTED, scheduler.getJob("huge").getState());
    }

    @Test
    @DisplayName("租户累计占用超过配额 -> TENANT_QUOTA_EXCEEDED（排队作业也计入配额）")
    void rejectWhenExceedTenantQuota() throws Exception {
        // 第一个作业占 6C（阻塞执行中），租户配额 8C
        log.info("[input] submit job=a tenant=t1 cpu=6 (will stay RUNNING)");
        SubmitResult a = scheduler.submit("a", "t1", 0, TestSupport.res(6, 1024));
        assertTrue(a.accepted());
        assertEquals(JobState.RUNNING, scheduler.getJob("a").getState());

        // 再申请 3C：6+3=9 > 8
        log.info("[input] submit job=b tenant=t1 cpu=3, tenant active=6 quota=8");
        SubmitResult b = scheduler.submit("b", "t1", 0, TestSupport.res(3, 1024));
        log.info("[decision] accepted={} reason={} message={}", b.accepted(), b.rejectReason(), b.message());

        assertFalse(b.accepted());
        assertEquals(RejectReason.TENANT_QUOTA_EXCEEDED, b.rejectReason());

        // 别的租户不受影响
        SubmitResult c = scheduler.submit("c", "t2", 0, TestSupport.res(8, 1024));
        log.info("[decision] other-tenant job=c accepted={}", c.accepted());
        assertTrue(c.accepted());
    }

    @Test
    @DisplayName("同一作业重复提交 -> DUPLICATE_JOB，只执行一次")
    void duplicateSubmissionRejected() throws Exception {
        log.info("[input] submit job=dedup twice with same id");
        SubmitResult first = scheduler.submit("dedup", "t1", 0, TestSupport.res(2, 256));
        SubmitResult second = scheduler.submit("dedup", "t1", 0, TestSupport.res(2, 256));
        log.info("[decision] first.accepted={} second.accepted={} second.reason={}",
                first.accepted(), second.accepted(), second.rejectReason());

        assertTrue(first.accepted());
        assertFalse(second.accepted());
        assertEquals(RejectReason.DUPLICATE_JOB, second.rejectReason());

        handler.releaseAll();
        TestSupport.waitFor(5000, () ->
                assertEquals(JobState.SUCCEEDED, scheduler.getJob("dedup").getState()));
        log.info("[assert] execution count of dedup = {}", handler.countOf("dedup"));
        assertEquals(1, handler.countOf("dedup"));
    }

    @Test
    @DisplayName("资源暂时不足 -> PENDING 并可查到具体阻塞原因；释放资源后成功分派")
    void pendingWhenResourceTemporarilyUnavailableThenDispatch() throws Exception {
        // n1 放 6C，n2 放 6C，剩余各 2C；再来一个 4C 的作业必须等待
        log.info("[input] submit job=x cpu=6 and job=y cpu=6 on two 8C nodes, then job=z cpu=4");
        scheduler.submit("x", "t1", 0, TestSupport.res(6, 1024));
        // 用不同租户绕开租户配额，专注验证物理资源
        scheduler.submit("y", "t2", 0, TestSupport.res(6, 1024));

        SubmitResult z = scheduler.submit("z", "t3", 0, TestSupport.res(4, 1024));
        assertTrue(z.accepted());
        assertEquals(JobState.PENDING, scheduler.getJob("z").getState());
        String blockReason = scheduler.getJob("z").getLastBlockReason();
        log.info("[decision] z pending, blockReason='{}'", blockReason);
        assertNotNull(blockReason);
        assertTrue(blockReason.contains("insufficient free resources"));

        // 释放 x，应立即触发分派
        log.info("[input] release job=x, expect z dispatched");
        handler.releaseAll();
        TestSupport.waitFor(5000, () ->
                assertEquals(JobState.SUCCEEDED, scheduler.getJob("x").getState()));
        TestSupport.waitFor(5000, () ->
                assertEquals(JobState.SUCCEEDED, scheduler.getJob("z").getState()));

        long totalUsed = scheduler.listNodes().stream()
                .mapToLong(n -> n.getUsed().cpu()).sum();
        log.info("[assert] cluster used cpu after drain = {}", totalUsed);
        assertEquals(0L, totalUsed);
        assertEquals(NodeState.ONLINE, scheduler.listNodes().get(0).getState());
    }
}
