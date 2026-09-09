package com.devansh.messagequeue.consumer.metrics;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/consumer-groups")
public class LoadMetricsController {
    private final LoadMetricsService loadMetricsService;
    public LoadMetricsController(LoadMetricsService loadMetricsService) {
        this.loadMetricsService = loadMetricsService;
    }

    @GetMapping("/{groupName}/load")
    public ConsumerGroupLoad getLoad(@PathVariable String groupName) {
        return loadMetricsService.getGroupLoad(groupName);
    }
}
