package com.example.kafkapublisher;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaConfig {

    @Value("${kafka.m4s-topic}")
    private String TOPIC_M4S_AFS;

    @Value("${kafka.econtract-afs-topic}")
    private String TOPIC_EC_AFS_TOPIC;

    @Value("${kafka.m4s-group-id}")
    public String groupId;

    @Bean
    public String getGroupId(){
        log.info("groupId: {}", groupId);
        return groupId;
    }

    @Bean
    public String getTopic(){
        log.info("topic: {}", TOPIC_M4S_AFS);
        return TOPIC_M4S_AFS;
    }

    @Bean
    public String getTopicEcontract(){
        log.info("topic econtract: {}", TOPIC_EC_AFS_TOPIC);
        return TOPIC_EC_AFS_TOPIC;
    }

    @Value("${kafka.bootstrap-servers}")
    private String BOOTSTRAP_SERVERS;

    @Value("${kafka.admin-user}")
    private String USERNAME;

    @Value("${kafka.admin-password}")
    private String PASSWORD;

    private final String SCRAM_SHA_512 = "SCRAM-SHA-512";
    private final String SASL_SSL = "SASL_SSL";
    private String PROD_JAAS_CFG;

    @PostConstruct
    private void setSaslJassConfigs(){
        String JAAS_TEMPLATE = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        PROD_JAAS_CFG  = String.format(JAAS_TEMPLATE, USERNAME, PASSWORD);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        log.info("producerFactory config info: username: {}, password: {}, topic: {}, \n bootstrap-server: {} \n prod-jaas: {}", USERNAME, PASSWORD, TOPIC_M4S_AFS, BOOTSTRAP_SERVERS, PROD_JAAS_CFG);
        final Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_SSL);
        configProps.put(SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512);
        configProps.put(SaslConfigs.SASL_JAAS_CONFIG, PROD_JAAS_CFG);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        log.info("consumerFactory config info: username: {}, password: {}, topic: {}\n bootstrap-server: {} \n prod-jaas: {}", USERNAME, PASSWORD, TOPIC_M4S_AFS, BOOTSTRAP_SERVERS, PROD_JAAS_CFG);
        final Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_SSL);
        configProps.put(SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512);
        configProps.put(SaslConfigs.SASL_JAAS_CONFIG, PROD_JAAS_CFG);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        log.info("kafkaAdmin config info: username: {}, password: {}, topic: {}, GROUP: {}\n bootstrap-server: {} \n prod-jaas: {}", USERNAME, PASSWORD, TOPIC_M4S_AFS, groupId, BOOTSTRAP_SERVERS, PROD_JAAS_CFG);
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_SSL);
        configs.put(SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512);
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, PROD_JAAS_CFG);
        return new KafkaAdmin(configs);
    }

    @Bean
    public AdminClient adminClient() {
        log.info("adminClient config info: username: {}, password: {}, topic: {}, GROUP: {}\n bootstrap-server: {} \n prod-jaas: {}", USERNAME, PASSWORD, TOPIC_M4S_AFS, groupId, BOOTSTRAP_SERVERS, PROD_JAAS_CFG);
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_SSL);
        configs.put(SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512);
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, PROD_JAAS_CFG);
        return  AdminClient.create(configs);
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(TOPIC_M4S_AFS)
                           .build();
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        final var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
