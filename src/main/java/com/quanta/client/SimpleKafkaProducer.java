//Producer API for quanta client
//article based implementation

package com.quanta.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SimpleKafkaProducer {
    private final SimpleKafkaClient client;
    private final String topic;

    public SimpleKafkaProducer(String bootstrapBroker,int bootstrapPort,String topic) {
        this.client = new SimpleKafkaClient(bootstrapBroker,bootstrapPort);
        this.topic = topic;
    }

    public void initialize() throws IOException {
        client.initialize(topic);
    }

    public long send(String message) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        return client.send(topic,0,data);
    }

    public long send(String message,int partition) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        return client.send(topic,partition,data);
    }

    public void close() {
    }
}