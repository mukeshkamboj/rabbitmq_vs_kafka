package com.learning.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.rabbitmq.client.BuiltinExchangeType.FANOUT;
import static java.nio.charset.StandardCharsets.UTF_8;

/*
Publisher publishes the message on exchange and queues bind to the exchange receive the copy of message.
 */
public class PublishSubscribeFanout {
    private static String EXCHANGE_NAME = PublishSubscribeFanout.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(() -> Producer.produce("Producer 1"));
        executor.execute(() -> Producer.produce("Producer 2"));
        Consumer.consume("Consumer-1");
        Consumer.consume("Consumer-2");
        Consumer.consume("Consumer-3");
        executor.shutdown();
    }

    private static class Producer {
        private static void produce(String producerName) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            long counter = 0;
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(EXCHANGE_NAME, FANOUT);
                while (true) {
                    String message = producerName + " - " + ++counter;
                    channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
                    System.out.println(" [x] Sent '" + message + "'");
                    Thread.sleep(500);
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private static class Consumer {
        private static void consume(String consumerName) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try {
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                channel.exchangeDeclare(EXCHANGE_NAME, FANOUT);
                String queue = channel.queueDeclare().getQueue();
                channel.queueBind(queue, EXCHANGE_NAME, "");
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), UTF_8);
                    System.out.println(consumerName + " received '" + message + "'");
                };
                channel.basicConsume(queue, true, deliverCallback, consumerTag -> {
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}