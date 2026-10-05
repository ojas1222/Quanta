//Consumer API for quanta client

package com.quanta.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleKafkaConsumer {
    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;
    private long currentOffset;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;
    private static final long POLL_INTERVAL_MS = 100;

    public SimpleKafkaConsumer(String bootstrapBroker,int bootstrapPort,String topic,int partition) {
        this.client = new SimpleKafkaClient(bootstrapBroker,bootstrapPort);
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = 0;
    }

    public void initialize() throws IOException {
        client.initialize(topic);
    }

    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic,partition,currentOffset,1024 * 1024);
        if (!messages.isEmpty()) {
            currentOffset += messages.size();
        }
        return messages;
    }
    public void seek(long offset) {
        this.currentOffset = offset;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    //message handler part
    public interface MessageHandler {
        void handle(byte[] message,long offset);
    }

    public void startConsuming(MessageHandler handler) {
        if (running.compareAndSet(false,true)) {
            consumerThread = new Thread(() -> {
                try {
                    while (running.get()) {
                        List<byte[]> messages = poll();

                        for (byte[] message : messages) {
                            handler.handle(message,currentOffset - messages.size() + messages.indexOf(message));
                        }

                        if (messages.isEmpty()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        e.printStackTrace();
                    }
                    running.set(false);
                }
            });

            consumerThread.setDaemon(true);
            consumerThread.start();
        }
    }

    public void stopConsuming() {
        running.set(false);

        if (consumerThread != null) {
            consumerThread.interrupt();
        }
    }
}