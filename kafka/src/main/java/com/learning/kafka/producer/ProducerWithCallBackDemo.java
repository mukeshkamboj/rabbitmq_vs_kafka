package com.learning.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerWithCallBackDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        IntStream.range(1, 1001).forEach(index -> {
            ProducerRecord<String, String> record1 = new ProducerRecord<>("first_topic", "Message : " + index);
            ProducerRecord<String, String> record2 = new ProducerRecord<>("second_topic", "Message : " + index);
            producer.send(record1, ProducerWithCallBackDemo::onCompletion);
            producer.send(record2, ProducerWithCallBackDemo::onCompletion);
            producer.flush();
        });
        producer.close();
    }

    private static void onCompletion(RecordMetadata metadata, Exception ex) {
        if (ex == null) {
            System.out.println(
                    "Topic : " + metadata.topic() + "\n" +
                            " Partition : " + metadata.partition() + "\n" +
                            "Offset : " + metadata.offset() + "\n"
            );
        } else {
            ex.printStackTrace();
        }
    }

}
