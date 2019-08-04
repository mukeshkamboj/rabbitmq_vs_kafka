package com.learning.rabbitmq;

import com.rabbitmq.client.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/* This demonstrate the multiple publishers/workers example.
Broker delivers one message to one worker and wait until worker says it is done with the processing.
This is to make sure that workers are sharing the load. We need to specify how many messages one broker wants to be delivered and processes.
 */
public class MultipleConsumer {
    private static String QUEUE_NAME = MultipleConsumer.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(() -> producer("Producer 1"));
        executor.execute(() -> producer("Producer 2"));
        consume("Consumer-1");
        consume("Consumer-2");
        consume("Consumer-3");
        executor.shutdown();
    }

    private static void producer(String producerName) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        long counter = 0;
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
            while (true) {
                String message = producerName + " - " + ++counter;
                // MessageProperties.PERSISTENT_TEXT_PLAIN make the message persisted.
                channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");
                Thread.sleep(500);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("========================================================================================================");
        }
    }

    private static void consume(String consumerName) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.basicQos(1);
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(consumerName + " received '" + message + "'");
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // +ve acknowledgement. Message will be removed from queue.
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                // -ve acknowledgement. Message will stay in queue.
                //channel.basicNack(delivery.getEnvelope().getDeliveryTag(), true, true);
            };
            boolean autoAcknowledge = false;
            channel.basicConsume(QUEUE_NAME, autoAcknowledge, deliverCallback, consumerTag -> {
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}