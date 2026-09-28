package com.github.h3yow.clusterjobschedulerjava.web;

import com.github.h3yow.clusterjobschedulerjava.scheduler.JobScheduler;
import com.github.h3yow.clusterjobschedulerjava.web.dto.NodeView;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 节点状态查询接口：容量、已分配、可分配与在跑作业。
 */
@RestController
@RequestMapping("/api/nodes")
public class NodeController {

    private final JobScheduler scheduler;

    public NodeController(JobScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @GetMapping
    public List<NodeView> list() {
        return scheduler.listNodes().stream().map(NodeView::from).toList();
    }

    @GetMapping("/{id}")
    public ResponseEntity<NodeView> get(@PathVariable String id) {
        var node = scheduler.getNode(id);
        if (node == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(NodeView.from(node));
    }
}
