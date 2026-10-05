//class to test SimpleKafkaProducers implemntation
package com.quanta.client;

public class ProducerTest {
    public static void main(String[] args) throws Exception {
        SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost",9092,"test-topic");
        producer.initialize();

        long offset = producer.send("Hello from Quanta Producer");

        System.out.println("produced message at offset: " + offset);

        producer.close();
    }
}