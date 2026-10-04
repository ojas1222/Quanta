package com.quanta.broker;

public class TopicTest {

    public static void main(String[] args) throws Exception {
        ZooKeeperClient zkClient = new ZooKeeperClient();

        zkClient.connect();

        zkClient.createTopicMetadata("test-topic", 1);
        zkClient.storePartitionMetadata("test-topic", 0, 0, new java.util.ArrayList<>());

        System.out.println("Created topic: test-topic");

        zkClient.close();
    }
}