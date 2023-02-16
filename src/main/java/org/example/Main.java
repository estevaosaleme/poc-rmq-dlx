package org.example;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class Main {

    public static void main(String[] args) throws IOException, TimeoutException {
        publish();
        consume();
        consumeDeadLetter();
    }

    private static void consume() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPort(30672);
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.basicConsume("poc-queue", false, "myConsumerTagPoc",
                new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body)
                            throws IOException
                    {
                        long deliveryTag = envelope.getDeliveryTag();
                        System.out.println("Received: " + new String(body, StandardCharsets.UTF_8));
                        channel.basicNack(deliveryTag, false, false);
                    }
                });

        //channel.close();
        //connection.close();
    }

    private static void consumeDeadLetter() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPort(30672);
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.basicConsume("poc-queue-dead-letter", true, "myConsumerTagPoc",
                new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        System.out.println("Dead-letter: " + new String(body, StandardCharsets.UTF_8));
                        System.out.println("Dead-letter headers: ");
                        properties.getHeaders().forEach((key, value) -> System.out.println(key + ": " + value));
                    }
                });

        //channel.close();
        //connection.close();
    }

    private static void publish() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPort(30672);
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        Channel channelDeadLetter = connection.createChannel();
        channelDeadLetter.exchangeDeclare("poc-exchange-dead-letter", "fanout", true);
        channelDeadLetter.queueDeclare("poc-queue-dead-letter", true, false, false, null);
        channelDeadLetter.queueBind("poc-queue-dead-letter", "poc-exchange-dead-letter", "");

        channel.exchangeDeclare("poc-exchange", "direct", true);

        Map<String, Object> props = new HashMap<String, Object>();
        props.put("x-dead-letter-exchange", "poc-exchange-dead-letter");
        //props.put("x-dead-letter-routing-key", "poc-queue-dead-letter-key");
        //props.put("x-message-ttl", 60000);

        channel.queueDeclare("poc-queue", true, false, false, props);
        channel.queueBind("poc-queue", "poc-exchange", "poc-queue");

        String message = "Hello World!";
        channel.basicPublish("poc-exchange", "poc-queue", null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println("Sent: " + message);
        channel.close();
        channelDeadLetter.close();
        connection.close();
    }
}