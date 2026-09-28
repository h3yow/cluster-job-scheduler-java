package com.github.h3yow.clusterjobschedulerjava.scheduler.web;

import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.ClusterNode;
import com.github.h3yow.clusterjobschedulerjava.scheduler.domain.Job;
import com.github.h3yow.clusterjobschedulerjava.scheduler.engine.SchedulerCore;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.CompletionOutcome;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.JobRequest;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.Resources;
import com.github.h3yow.clusterjobschedulerjava.scheduler.model.SubmitResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 本地批处理作业调度的 HTTP 接口。 */
@RestController
@RequestMapping("/api/scheduler")
public final class SchedulerController {

    private final SchedulerCore scheduler;

    public SchedulerController(SchedulerCore scheduler) {
        this.scheduler = scheduler;
    }

    @PostMapping("/jobs")
    public SubmitResult submit(@RequestBody JobRequest request) {
        return scheduler.submit(request);
    }

    @PostMapping("/jobs/{jobId}/cancel")
    public Map<String, Object> cancel(@PathVariable String jobId,
                                      @RequestParam(defaultValue = "user requested") String reason) {
        boolean ok = scheduler.cancel(jobId, reason);
        return Map.of("jobId", jobId, "cancelled", ok);
    }

    @PostMapping("/jobs/{jobId}/completion")
    public Map<String, Object> reportCompletion(@PathVariable String jobId,
                                                @RequestParam String outcome,
                                                @RequestParam(defaultValue = "") String detail) {
        CompletionOutcome result;
        try {
            result = CompletionOutcome.valueOf(outcome.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return Map.of("jobId", jobId, "accepted", false, "error", "invalid outcome: " + outcome);
        }
        boolean ok = scheduler.reportCompletion(jobId, result, detail);
        return Map.of("jobId", jobId, "accepted", ok);
    }

    /** 手动触发一轮分派（通常提交 / 取消 / 回报后已自动触发，这里便于观察与兜底）。 */
    @PostMapping("/schedule")
    public Map<String, Object> schedule() {
        int assigned = scheduler.schedulePending();
        return Map.of("newlyAssigned", assigned);
    }

    @GetMapping("/jobs/{jobId}")
    public Map<String, Object> getJob(@PathVariable String jobId) {
        return scheduler.getJob(jobId)
                .map(SchedulerController::toJobView)
                .orElseGet(() -> Map.of("jobId", jobId, "found", false));
    }

    @GetMapping("/jobs")
    public List<Map<String, Object>> listJobs() {
        return scheduler.listJobs().stream()
                .map(SchedulerController::toJobView)
                .toList();
    }

    @GetMapping("/nodes")
    public List<Map<String, Object>> nodes() {
        return scheduler.getCluster().nodes().stream()
                .map(SchedulerController::toNodeView)
                .toList();
    }

    private static Map<String, Object> toJobView(Job job) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("jobId", job.getId());
        view.put("tenant", job.getTenant());
        view.put("name", job.getName());
        view.put("state", job.getState());
        view.put("terminal", job.getState().isTerminal());
        view.put("priority", job.getPriority());
        view.put("required", toResourcesView(job.getRequired()));
        view.put("attempt", job.getAttempt());
        view.put("maxAttempts", job.getMaxAttempts());
        view.put("assignedNodeId", job.getAssignedNodeId());
        view.put("rejectReason", job.getRejectReason());
        view.put("terminalReason", job.getTerminalReason());
        view.put("lastScheduleSkip", job.getLastScheduleSkip());
        view.put("events", job.getEvents().stream().map(e -> {
            Map<String, Object> ev = new LinkedHashMap<>();
            ev.put("timestampMs", e.timestampMs());
            ev.put("type", e.type());
            ev.put("detail", e.detail());
            return ev;
        }).toList());
        return view;
    }

    private static Map<String, Object> toNodeView(ClusterNode node) {
        Resources used = node.getUsed();
        Resources available = node.available();
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("nodeId", node.getId());
        view.put("capacity", toResourcesView(node.getCapacity()));
        view.put("used", toResourcesView(used));
        view.put("available", toResourcesView(available));
        view.put("runningJobIds", node.getRunningJobIds());
        return view;
    }

    private static Map<String, Object> toResourcesView(Resources r) {
        return Map.of("cpuCores", r.getCpuCores(), "memoryMb", r.getMemoryMb());
    }
}
