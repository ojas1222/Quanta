package com.quanta.client;

import java.util.List;

public class ClientTest {
    public static void main(String[] args) throws Exception {
        SimpleKafkaClient client = new SimpleKafkaClient("localhost", 9092);

        String topic = "test-topic";

        client.initialize(topic);

        String message = "Hello from Quanta Client";

        long offset = client.send(topic, 0, message.getBytes());

        System.out.println("Produced message at offset: " + offset);

        List<byte[]> messages = client.fetch(topic, 0, offset, 1024);

        for (byte[] data : messages) {
            System.out.println("Fetched message: " + new String(data));
        }
    }
}