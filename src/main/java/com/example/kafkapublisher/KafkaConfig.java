package com.example.kafkapublisher;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    public static final String TOPIC_1 = "M4S_DEFAULT";
    public static final String bootstrapServers = "b-3.dev-kafka.d9daxy.c4.kafka.ap-southeast-1.amazonaws.com:9096,b-2.dev-kafka.d9daxy.c4.kafka.ap-southeast-1.amazonaws.com:9096,b-1.dev-kafka.d9daxy.c4.kafka.ap-southeast-1.amazonaws.com:9096";
//    public static final String bootstrapServers = "b-1.uat-kafka.khwnx8.c4.kafka.ap-southeast-1.amazonaws.com:9096,b-2.uat-kafka.khwnx8.c4.kafka.ap-southeast-1.amazonaws.com:9096";
    private static final String USERNAME = "admin-kafka";
    private static final String PASSWORD = "aNyS9ub@NE3M6H";
//    private static final String PASSWORD = "W7pSWiyHMkV*pg";

    private static final String SCRAM_SHA_512 = "SCRAM-SHA-512";
    private static final String SASL_SSL = "SASL_SSL";
    private final String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
    private final String prodJaasCfg = String.format(jaasTemplate, USERNAME, PASSWORD);

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        final Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_SSL);
        configProps.put(SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512);
        configProps.put(SaslConfigs.SASL_JAAS_CONFIG, prodJaasCfg);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_SSL);
        props.put(SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
                "%s required username=\"%s\" " + "password=\"%s\";", PlainLoginModule.class.getName(), USERNAME, PASSWORD
        ));

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_SSL);
        configs.put(SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512);
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, prodJaasCfg);
        return new KafkaAdmin(configs);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }
}
