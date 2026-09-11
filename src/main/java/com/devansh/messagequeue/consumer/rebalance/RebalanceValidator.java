package com.devansh.messagequeue.consumer.rebalance;

import com.devansh.messagequeue.consumer.ConsumerGroup;
import com.devansh.messagequeue.consumer.metrics.PartitionLoad;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class RebalanceValidator {

    public void validate(
            ConsumerGroup group,
            RebalancePlan plan,
            List<PartitionLoad> partitionLoads
    ) {

        if (group == null) {
            throw new IllegalArgumentException(
                    "Consumer group does not exist"
            );
        }

        if (!group.getName().equals(plan.groupName())) {
            throw new IllegalArgumentException(
                    "Plan belongs to a different consumer group"
            );
        }

        /*
         * Important stale-plan protection.
         *
         * If someone joined/left after simulation,
         * assignments would have changed.
         */
        if (!group.getAssignments()
                .equals(plan.currentAssignments())) {

            throw new IllegalStateException(
                    "Rebalance plan is stale. " +
                            "Consumer assignments have changed."
            );
        }

        Map<Integer, String> proposed =
                plan.proposedAssignments();

        Set<Integer> expectedPartitions =
                new HashSet<>();

        for (PartitionLoad load : partitionLoads) {
            expectedPartitions.add(
                    load.partitionId()
            );
        }

        /*
         * Every partition must exist exactly once.
         *
         * Since we're using a Map, duplicate
         * partition IDs cannot exist.
         */
        if (!proposed.keySet()
                .equals(expectedPartitions)) {

            throw new IllegalArgumentException(
                    "Proposed assignments do not contain " +
                            "exactly the required partitions"
            );
        }

        Set<String> activeConsumers =
                group.getConsumers();

        for (Map.Entry<Integer, String> entry :
                proposed.entrySet()) {

            String consumer =
                    entry.getValue();

            if (consumer == null) {
                throw new IllegalArgumentException(
                        "Partition " +
                                entry.getKey() +
                                " has no consumer assigned"
                );
            }

            if (!activeConsumers.contains(consumer)) {
                throw new IllegalArgumentException(
                        "Unknown or inactive consumer: "
                                + consumer
                );
            }
        }
    }
}