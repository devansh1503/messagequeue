package com.devansh.messagequeue.consumer;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/consumer-groups")
public class ConsumerGroupController {
    private final ConsumerGroupService consumerGroupService;
    public ConsumerGroupController(ConsumerGroupService consumerGroupService) {
        this.consumerGroupService = consumerGroupService;
    }
    @PostMapping("/{group}/offsets")
    public void commit(@PathVariable String group, @RequestBody CommitOffsetRequest request){
        consumerGroupService.commit(
                group,
                request.topic(),
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
        return consumerGroupService.getCommitedOffset(group,topic,partition);
    }
}
