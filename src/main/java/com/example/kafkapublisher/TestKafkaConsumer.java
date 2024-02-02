package com.example.kafkapublisher;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequiredArgsConstructor
public class TestKafkaConsumer {

  private final KafkaCustomer<String, String> kafkaCustomer;
  private final Consumer<String, String> consumer;

  @GetMapping("/get-message-from-offset")
  public String getMessageFromOffset(@RequestParam String topic,
      @RequestParam int partition,
      @RequestParam long offset) {
    log.info("getMessageFromOffset() called with: [{}], [{}], [{}]", topic, partition, offset);
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    List<TopicPartition> partitions = new ArrayList<>();
    partitions.add(topicPartition);
    consumer.assign(partitions);
    consumer.seek(topicPartition, offset);

    //config thời gian chờ nếu chưa có message tại offset chỉ định.
    ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(100));
    final List<ConsumerRecord<String, String>> records = messages.records(topicPartition);
    log.info("records size: [{}]", records.size());
    return records.get(0).value();
  }

  @GetMapping("/v2/get-message-from-offset")
  public String getMessageFromOffsetV2(@RequestParam String topic,
      @RequestParam int partition,
      @RequestParam long offset) throws InterruptedException {
    log.info("getMessageFromOffset() called with: [{}], [{}], [{}]", topic, partition, offset);

    final ConsumerRecord<String,String> consumerRecord = kafkaCustomer.get(topic, partition, offset);

    log.info("records size: [{}]", consumerRecord);
    return consumerRecord.value();
  }
}
