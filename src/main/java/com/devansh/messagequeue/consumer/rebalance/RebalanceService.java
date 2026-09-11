package com.devansh.messagequeue.consumer.rebalance;

import com.devansh.messagequeue.consumer.ConsumerGroup;
import com.devansh.messagequeue.consumer.ConsumerGroupService;
import com.devansh.messagequeue.consumer.metrics.ConsumerGroupLoad;
import com.devansh.messagequeue.consumer.metrics.LoadMetricsService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

@Service
public class RebalanceService {

    private static final double REBALANCE_THRESHOLD = 1.20;

    private final ConsumerGroupService consumerGroupService;
    private final LoadMetricsService loadMetricsService;

    private final RoundRobinAssignmentStrategy roundRobinAssignmentStrategy;
    private final LoadAwareAssignmentStrategy loadAwareAssignmentStrategy;
    private final ImbalanceCalculator imbalanceCalculator;
    private final RebalanceValidator rebalanceValidator;

    public RebalanceService(
            ConsumerGroupService consumerGroupService,
            LoadMetricsService loadMetricsService,
            RoundRobinAssignmentStrategy roundRobinAssignmentStrategy,
            LoadAwareAssignmentStrategy loadAwareAssignmentStrategy,
            ImbalanceCalculator imbalanceCalculator,
            RebalanceValidator rebalanceValidator
    ) {

        this.consumerGroupService = consumerGroupService;
        this.loadMetricsService = loadMetricsService;
        this.roundRobinAssignmentStrategy = roundRobinAssignmentStrategy;
        this.loadAwareAssignmentStrategy = loadAwareAssignmentStrategy;
        this.imbalanceCalculator = imbalanceCalculator;
        this.rebalanceValidator = rebalanceValidator;
    }

    /*
     * SIMULATION ONLY.
     *
     * Does NOT modify group assignments.
     */
    public RebalanceSimulation simulate(String groupName, String strategyName) {

        ConsumerGroup group = getGroup(groupName);

        ConsumerGroupLoad groupLoad = loadMetricsService.getGroupLoad(groupName);

        List<String> consumers = new ArrayList<>(group.getConsumers());

        ConsumerAssignmentStrategy strategy = resolveStrategy(strategyName);

        Map<Integer, String> currentAssignments = new TreeMap<>(group.getAssignments());

        Map<Integer, String> proposedAssignments = new TreeMap<>(strategy.assign(consumers, groupLoad.partitions()));

        int partitionsMoved = calculateMovedPartitions(currentAssignments, proposedAssignments);

        RebalancePlan plan = new RebalancePlan(
                        groupName,
                        strategyName.toUpperCase(),
                        currentAssignments,
                        proposedAssignments,
                        partitionsMoved
                );

        /*
         * Validate even simulation output.
         *
         * A strategy producing an invalid assignment
         * should be caught immediately.
         */
        rebalanceValidator.validate(
                group,
                plan,
                groupLoad.partitions()
        );

        double currentImbalance =
                imbalanceCalculator.calculate(
                        consumers,
                        groupLoad.partitions(),
                        currentAssignments
                );

        double projectedImbalance =
                imbalanceCalculator.calculate(
                        consumers,
                        groupLoad.partitions(),
                        proposedAssignments
                );

        double improvementPercent =
                calculateImprovement(
                        currentImbalance,
                        projectedImbalance
                );

        boolean rebalanceRecommended =
                shouldRebalance(
                        currentImbalance,
                        projectedImbalance,
                        partitionsMoved
                );

        return new RebalanceSimulation(
                plan,
                currentImbalance,
                projectedImbalance,
                improvementPercent,
                rebalanceRecommended
        );
    }

    /*
     * Explicit mutation.
     *
     * Should only happen after simulation/approval.
     */
    public RebalanceResult apply(
            RebalancePlan plan
    ) {

        ConsumerGroup group =
                getGroup(plan.groupName());

        ConsumerGroupLoad groupLoad =
                loadMetricsService
                        .getGroupLoad(
                                plan.groupName()
                        );

        rebalanceValidator.validate(
                group,
                plan,
                groupLoad.partitions()
        );

        Map<Integer, String> previousAssignments =
                new TreeMap<>(
                        group.getAssignments()
                );

        /*
         * Actual mutation happens ONLY here.
         */
        group.getAssignments().clear();

        group.getAssignments().putAll(
                plan.proposedAssignments()
        );

        return new RebalanceResult(
                plan.groupName(),
                plan.strategy(),
                previousAssignments,
                new TreeMap<>(
                        plan.proposedAssignments()
                ),
                plan.partitionsMoved()
        );
    }

    private ConsumerGroup getGroup(
            String groupName
    ) {

        ConsumerGroup group =
                consumerGroupService
                        .getGroup(groupName);

        if (group == null) {
            throw new IllegalArgumentException(
                    "Consumer group not found: "
                            + groupName
            );
        }

        if (group.getConsumers().isEmpty()) {
            throw new IllegalStateException(
                    "Consumer group has no active consumers: "
                            + groupName
            );
        }

        return group;
    }

    private ConsumerAssignmentStrategy resolveStrategy(
            String strategyName
    ) {

        if (strategyName == null) {
            throw new IllegalArgumentException(
                    "strategyName cannot be null"
            );
        }

        return switch (
                strategyName.toUpperCase()
                ) {

            case "ROUND_ROBIN" ->
                    roundRobinAssignmentStrategy;

            case "LOAD_AWARE" ->
                    loadAwareAssignmentStrategy;

            default ->
                    throw new IllegalArgumentException(
                            "Unknown strategy: "
                                    + strategyName
                    );
        };
    }

    private int calculateMovedPartitions(
            Map<Integer, String> previousAssignments,
            Map<Integer, String> newAssignments
    ) {

        int moved = 0;

        for (Map.Entry<Integer, String> entry :
                newAssignments.entrySet()) {

            String previousConsumer =
                    previousAssignments.get(
                            entry.getKey()
                    );

            if (!Objects.equals(
                    previousConsumer,
                    entry.getValue()
            )) {
                moved++;
            }
        }

        return moved;
    }

    private double calculateImprovement(
            double current,
            double projected
    ) {

        if (current <= 0) {
            return 0;
        }

        double improvement =
                ((current - projected) / current)
                        * 100;

        return Math.max(0, improvement);
    }

    private boolean shouldRebalance(
            double currentImbalance,
            double projectedImbalance,
            int partitionsMoved
    ) {

        /*
         * Nothing moves = nothing to do.
         */
        if (partitionsMoved == 0) {
            return false;
        }

        /*
         * Current assignment is already reasonably
         * balanced.
         */
        if (currentImbalance < REBALANCE_THRESHOLD) {
            return false;
        }

        /*
         * New assignment must actually improve things.
         */
        return projectedImbalance < currentImbalance;
    }
}