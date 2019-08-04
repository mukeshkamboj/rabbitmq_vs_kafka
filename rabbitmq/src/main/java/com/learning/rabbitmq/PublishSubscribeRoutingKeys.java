package com.learning.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.rabbitmq.client.BuiltinExchangeType.DIRECT;

/*
Publisher publishes the message on exchange and queues bind to the exchange receive the copy of message with associated routing keys.
 */
public class PublishSubscribeRoutingKeys {
    private static String EXCHANGE_NAME = PublishSubscribeRoutingKeys.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(() -> producer("Producer 1", "key-1"));
        executor.execute(() -> producer("Producer 2", "key-2"));
        consume("Consumer-1", "key-1");
        consume("Consumer-2", "key-2");
        consume("Consumer-3", "key-1", "key-2");
        executor.shutdown();
    }

    private static void producer(String producerName, String key) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        long counter = 0;
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, DIRECT);
            while (true) {
                String message = producerName + " - " + ++counter;
                channel.basicPublish(EXCHANGE_NAME, key, null, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");
                Thread.sleep(500);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("========================================================================================================");
        }
    }

    private static void consume(String consumerName, String... routingKeys) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, DIRECT);
            String queue = channel.queueDeclare().getQueue();
            for (String key : routingKeys) {
                channel.queueBind(queue, EXCHANGE_NAME, key);
            }
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(consumerName + " received '" + message + "'" + " for " + delivery.getEnvelope().getRoutingKey());
            };
            channel.basicConsume(queue, true, deliverCallback, consumerTag -> {
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}