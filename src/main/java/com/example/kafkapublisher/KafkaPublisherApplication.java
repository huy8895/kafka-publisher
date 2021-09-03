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
    private final String TOPIC = "huy-test";
    @Autowired
    private KafkaTemplate<String, Object> template;

    @GetMapping("/publish/{name}")
    public String publishMessage(@PathVariable String name){
        log.info("publish message: {}", name);
        template.send(TOPIC,"Hi " + name);
        return "Data published";
    }

    @PostMapping("/publishJson")
    public String publishJsonMessage(@RequestBody User user) throws JsonProcessingException {
        log.info("publish json message: {}", user);
        template.send(TOPIC, user);
        return "Data json published";
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaPublisherApplication.class, args);
    }

}
