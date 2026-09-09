package com.devansh.messagequeue.consumer.rebalance;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/consumer-group")
public class RebalanceController {
    private final RebalanceService rebalanceService;
    public RebalanceController(RebalanceService rebalanceService) {
        this.rebalanceService = rebalanceService;
    }

    @PostMapping("/{groupName}/rebalance")
    public RebalanceResult rebalance(
            @PathVariable("groupName") String groupName,
            @RequestParam(defaultValue = "LOAD_AWARE") String strategy
    ) {
        return rebalanceService.rebalance(
                groupName,
                strategy
        );
    }
}
