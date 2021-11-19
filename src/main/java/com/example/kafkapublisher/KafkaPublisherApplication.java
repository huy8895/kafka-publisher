package com.example.kafkapublisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@SpringBootApplication
@RestController
@Slf4j
public class KafkaPublisherApplication {
    @Autowired
    private KafkaTemplate<String, Object> template;

    @GetMapping("/publish/{name}")
    public String publishMessage(@PathVariable String name){
        log.info("publish message: {} -> to topic: {}", name, KafkaConfig.TOPIC_1);
        template.send(KafkaConfig.TOPIC_1,"Hi " + name);
        return "Data published";
    }

    @PostMapping("/publishJson")
    public String publishJsonMessage(@RequestBody Object dto, @RequestParam("topic") String topic) {
        log.info("publish json message: \n{} -> to topic: {}", dto, topic);
        final var future = template.send(topic, dto);
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
