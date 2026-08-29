package com.devansh.messagequeue.cluster;

import com.devansh.messagequeue.broker.PartitionMetaData;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/cluster")
public class ClusterController {
    private final ClusterMetadataService clusterMetadataService;

    public ClusterController(ClusterMetadataService clusterMetadataService) {
        this.clusterMetadataService = clusterMetadataService;
    }

    @GetMapping("/topics/{topic}/partitions/{partition}")
    public PartitionMetaData getPartitionMetaData(@PathVariable String topic, @PathVariable int partition) {
        return clusterMetadataService.getPartitionMetaData(topic, partition);
    }
}
