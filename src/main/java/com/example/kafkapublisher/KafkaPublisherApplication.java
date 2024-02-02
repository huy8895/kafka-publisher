package com.example.kafkapublisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@SpringBootApplication
@RestController
@Slf4j
@RequiredArgsConstructor
public class KafkaPublisherApplication {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KafkaTemplate<String, Object> template;
    private final AdminClient adminClient;
    private final ObjectMapper objectMapper;

    @GetMapping("/list-topic")
    public ResponseEntity<Object> listTopic() throws Exception{
        log.info("start list topic");
        final var listTopicsResult1 = adminClient.listTopics();
        listTopicsResult1.listings().get().forEach(topicListing -> System.out.println("topicListing = " + topicListing.name()));
        final var listTopicsResult = listTopicsResult1.names().get();
        return ResponseEntity.ok(listTopicsResult);
    }

    @GetMapping("/topic-detail")
    public ResponseEntity<Object> topicDetail(@RequestParam(required = false) String topic) throws Exception {
        if (ObjectUtils.isEmpty(topic)) {
            final var list = new ArrayList<Object>();
            adminClient.listTopics()
                       .listings()
                       .get()
                       .forEach(topicListing -> list.add(getTopicDetail(topicListing.name())));
            return ResponseEntity.ok(list);
        } else {
            final var topicDetail = getTopicDetail(topic);
            return ResponseEntity.ok(topicDetail);
        }
    }

    @DeleteMapping("/delete-topic")
    public ResponseEntity<Object> deleteTopic(@RequestParam() String topic) throws Exception {
        if (adminClient.deleteTopics(List.of(topic)).all().isDone()) {
            return ResponseEntity.ok("deleted " + topic);
        } else {
            throw new Exception("delete false");
        }
    }

    @PutMapping("/update-topic")
    public void updateTopic(@RequestParam() String topic, @RequestParam() int numPartitions){
        Map<String, NewPartitions> newPartitionSet = new HashMap<>();
        newPartitionSet.put(topic, NewPartitions.increaseTo(numPartitions));
        adminClient.createPartitions(newPartitionSet);
        adminClient.close();
    }

    public Object getTopicDetail(String topic) {
        try {
            final var descriptionMap = adminClient.describeTopics(List.of(topic))
                                                  .all()
                                                  .get();
            final var hashMap = (HashMap) descriptionMap;
            final var o = (TopicDescription) hashMap.get(topic);
            return TopicDetail.builder()
                       .name(o.name())
                       .partitions(o.partitions()
                                    .size())
                       .partitionInfo(setPartitionInfo(o.partitions()))
                       .build();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return "exception";
    }

    private List<PartitionInfo> setPartitionInfo(List<TopicPartitionInfo> partitions) {
        return partitions.stream()
                         .map(topicPartitionInfo -> {
                                     final var leader = topicPartitionInfo.leader();
                                     return PartitionInfo.builder()
                                                         .partition(topicPartitionInfo.partition())
                                                         .leader(Node.builder()
                                                                     .id(leader.id())
                                                                     .idString(leader.idString())
                                                                     .host(leader.host())
                                                                     .port(leader.port())
                                                                     .host(leader.host())
                                                                     .rack(leader.rack())
                                                                     .build())
                                                         .build();
                                 }
                         )
                         .collect(Collectors.toList());
    }

    @GetMapping("/publish")
    public String publishMessage(@RequestParam String topic,
        @RequestParam String message){
        log.info("publish message:  -> to topic: {} ,[{}]", topic, message);
        Message<String> messageKafka = MessageBuilder
            .withPayload(message)
            .setHeader(KafkaHeaders.MESSAGE_KEY, UUID.randomUUID().toString())
            .setHeader(KafkaHeaders.TOPIC, topic)
            .setHeader(KafkaHeaders.RECEIVED_TOPIC, topic)
            .build();

        template.send(messageKafka)
            .addCallback(result -> log.info("send json message success: {}", result),
                throwable -> log.error("exception: ", throwable));
        return "Data published";
    }

    @PostMapping("/publishJson")
    public String publishJsonMessage(@RequestBody Object dto, @RequestParam("topic") String topic) throws Exception{
        log.info("publish json message: \n{} -> to topic: {}", OBJECT_MAPPER.writeValueAsString(dto), topic);
        final var future = template.send("M4S_DEFAULT", dto);
        future.addCallback(result -> {
                    log.info("send json message success: {}", result);
                },
                throwable -> {
                    log.error("exception: ", throwable);
                });
        return "Data json published";
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaPublisherApplication.class, args);
    }

}
