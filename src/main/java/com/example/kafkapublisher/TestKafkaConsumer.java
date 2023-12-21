package com.example.kafkapublisher;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
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

@SpringBootApplication
@RestController
@Slf4j
public class TestKafkaConsumer {

  public static final String DEFAULT_TOPIC = "test.topic.default";
  @Value("${kafka.bootstrap-servers}")
  private String BOOTSTRAP_SERVERS;

  @Bean
  public Consumer<String, String> consumer() {
    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup1");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
    return consumer;
  }

  @GetMapping("/get-message-from-offset")
  public String getMessageFromOffset(@RequestParam String topic,
      @RequestParam int partition,
      @RequestParam long offset) {
    log.info("getMessageFromOffset() called with: [{}], [{}], [{}]", topic, partition, offset);
    Consumer<String, String> consumer = consumer();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    List<TopicPartition> partitions = new ArrayList<>();
    partitions.add(topicPartition);
    consumer.assign(partitions);
    consumer.seek(topicPartition, offset);
    ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
    final List<ConsumerRecord<String, String>> records = messages.records(topicPartition);
    return records.get(0).value();
  }
}
