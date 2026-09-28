package com.github.h3yow.clusterjobschedulerjava.web;

import com.github.h3yow.clusterjobschedulerjava.model.Job;
import com.github.h3yow.clusterjobschedulerjava.model.Resources;
import com.github.h3yow.clusterjobschedulerjava.scheduler.JobScheduler;
import com.github.h3yow.clusterjobschedulerjava.scheduler.SubmitResult;
import com.github.h3yow.clusterjobschedulerjava.web.dto.JobSubmitRequest;
import com.github.h3yow.clusterjobschedulerjava.web.dto.JobView;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 作业提交/查询/取消接口。
 * <ul>
 *   <li>POST 202：已接受（PENDING 或已分派 RUNNING）；</li>
 *   <li>POST 409：重复提交 DUPLICATE_JOB；</li>
 *   <li>POST 422：配额/容量拒绝，body 中带明确 rejectReason；</li>
 *   <li>POST 400：请求参数非法；</li>
 *   <li>取消终态作业返回 409，作业不存在返回 404。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobScheduler scheduler;

    public JobController(JobScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostMapping
    public ResponseEntity<?> submit(@RequestBody JobSubmitRequest request) {
        if (request == null || request.id() == null || request.id().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "INVALID_REQUEST", "message", "id is required"));
        }
        Long cpu = request.cpu();
        Long memoryMb = request.memoryMb();
        if (cpu == null || memoryMb == null || cpu <= 0 || memoryMb <= 0) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "INVALID_REQUEST",
                    "message", "cpu and memoryMb are required and must be positive"));
        }
        int priority = request.priority() == null ? 0 : request.priority();
        SubmitResult result = scheduler.submit(
                request.id(), request.tenant(), priority, new Resources(cpu, memoryMb));

        if (!result.accepted()) {
            HttpStatus status = switch (result.rejectReason()) {
                case DUPLICATE_JOB -> HttpStatus.CONFLICT;
                default -> HttpStatus.UNPROCESSABLE_ENTITY;
            };
            return ResponseEntity.status(status).body(result);
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(result);
    }

    @GetMapping
    public List<JobView> list() {
        return scheduler.listJobs().stream().map(JobView::from).toList();
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobView> get(@PathVariable String id) {
        Job job = scheduler.getJob(id);
        if (job == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(JobView.from(job));
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<JobView> cancel(@PathVariable String id) {
        Job job = scheduler.getJob(id);
        if (job == null) {
            return ResponseEntity.notFound().build();
        }
        Job cancelled = scheduler.cancel(id);
        if (cancelled == null) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(JobView.from(job));
        }
        return ResponseEntity.ok(JobView.from(cancelled));
    }
}
