package com.learning.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        String kafkaServer = "localhost:9092";
        String groupId = ConsumerDemo.class.getSimpleName();
        String offsetConfig = "earliest";
        String topic = "first_topic";
        properties.put(BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(AUTO_OFFSET_RESET_CONFIG, offsetConfig);
        properties.put(GROUP_ID_CONFIG, groupId);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic, "second_topic"));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Key : " + record.key());
                System.out.println("Topic : " + record.topic() + " Partition : " + record.partition() + " " + "Offset : " + record.offset() + "\n");
            }
        }

    }
}
