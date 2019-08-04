package com.learning.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleProducerConsumer {
    private static String QUEUE_NAME = SimpleProducerConsumer.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(SimpleProducerConsumer::producer);
        consume();
        executor.shutdown();
    }

    private static void producer() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        while(true) {
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                String message = "Hello World!";
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");
                Thread.sleep(100);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.out.println("========================================================================================================");
            }
        }
    }

    private static void consume() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            };
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}