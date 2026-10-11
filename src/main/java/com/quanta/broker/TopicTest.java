
package com.quanta.broker;
import java.util.ArrayList;
public class TopicTest {
    public static void main(String[] args) throws Exception {
        ZooKeeperClient zkClient = new ZooKeeperClient();
        zkClient.connect();
        try {
            zkClient.createTopicMetadata("test-topic", 1);
            zkClient.storePartitionMetadata("test-topic", 0, 0, new ArrayList<>());
            System.out.println("Created topic: test-topic");
        } finally {
            zkClient.close();
        }
    }
}
