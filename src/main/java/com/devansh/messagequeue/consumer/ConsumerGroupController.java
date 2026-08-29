package com.devansh.messagequeue.consumer;

import com.devansh.messagequeue.message.Message;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/consumer-groups")
@CrossOrigin(origins = "http://localhost:5173")
public class ConsumerGroupController {
    private final ConsumerGroupService consumerGroupService;
    public ConsumerGroupController(ConsumerGroupService consumerGroupService) {
        this.consumerGroupService = consumerGroupService;
    }

    @PostMapping("/{group}/join")
    public ConsumerGroup join(@PathVariable String group, @RequestBody JoinGroupRequest request){
        return consumerGroupService.join(group, request.topic(), request.consumerId());
    }

    @DeleteMapping("/{group}/consumers/{consumerId}")
    public void leave(@PathVariable String group, @PathVariable String consumerId){
        consumerGroupService.leave(group, consumerId);
    }

    @GetMapping("/{group}/consumers/{consumerId}/partitions")
    public List<Integer> partitions(@PathVariable String group, @PathVariable String consumerId){
        return consumerGroupService.getAssignedPartitions(group, consumerId);
    }

    @GetMapping("/{group}/consumers/{consumerId}/messages")
    public Map<Integer, List<Message>> messages(
            @PathVariable String group,
            @PathVariable String consumerId,
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(defaultValue = "30000") long timeout
    ){
        return consumerGroupService.longPoll(group, consumerId, limit, timeout);
    }

    @PostMapping("/{group}/offsets")
    public void commit(@PathVariable String group, @RequestBody CommitOffsetRequest request){
        consumerGroupService.commit(
                group,
                request.partition(),
                request.offset()
        );
    }

    @GetMapping("/{group}/offsets")
    public long offset(
            @PathVariable String group,
            @RequestParam String topic,
            @RequestParam int partition
    ){
        return consumerGroupService.getCommitedOffset(group,partition);
    }

    @PostMapping("/{group}/consumers/{consumerId}/heartbeat")
    public void heartbeat(@PathVariable String group, @PathVariable String consumerId){
        consumerGroupService.heartbeat(group, consumerId);
    }

    @GetMapping
    public List<ConsumerGroupSummary> getGroups(){
        return consumerGroupService.getGroups();
    }

    @GetMapping("/{group}")
    public ConsumerGroupInfo getGroup(
            @PathVariable String group
    ){
        return consumerGroupService
                .getGroupInfo(group);
    }


}
