package com.devansh.messagequeue.consumer.rebalance;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/consumer-groups")
public class RebalanceController {

    private final RebalanceService rebalanceService;

    public RebalanceController(RebalanceService rebalanceService) {
        this.rebalanceService = rebalanceService;
    }

    @GetMapping("/{groupName}/rebalance/simulate")
    public RebalanceSimulation simulate(@PathVariable String groupName, @RequestParam(defaultValue = "LOAD_AWARE") String strategy
    ) {
        return rebalanceService.simulate(
                groupName,
                strategy
        );
    }

    @PostMapping("/{groupName}/rebalance/apply")
    public RebalanceResult apply(@PathVariable String groupName, @RequestBody RebalancePlan plan) {
        if (!groupName.equals(plan.groupName())) {
            throw new IllegalArgumentException(
                    "Group name in URL does not match plan"
            );
        }
        return rebalanceService.apply(plan);
    }
}