package com.devansh.messagequeue.consumer.rebalance;

public record RebalanceSimulation(
        RebalancePlan plan,
        double currentImbalance,
        double projectedImbalance,
        double improvementPercent,
        boolean rebalanceRecommended
) {
}