//conducted multiple tests to cerify behaviour of Consumer api with Producer api , running on Broker Launcher 0.
//catch-up + continuous consumption test for Consumer Api

package com.quanta.client;

import java.util.List;

public class ConsumerTest {
    public static void main(String[] args) throws Exception {
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost",9092,"test-topic",0);
        consumer.initialize();

        consumer.seek(0);

        while (true) {
            List<byte[]> messages = consumer.poll();

            if (messages.isEmpty()) {
                break;
            }

            Thread.sleep(100);
        }

        System.out.println("consumer caught up at offset: " + consumer.getCurrentOffset());

        consumer.startConsuming((message,offset) -> {
            System.out.println("new message at offset " + offset + ": " + new String(message));
        });

        Thread.sleep(10000);

        consumer.stopConsuming();

        System.out.println("consumer stopped.");
    }
}