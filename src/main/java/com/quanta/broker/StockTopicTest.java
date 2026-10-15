
package com.quanta.broker;

import java.util.ArrayList;

public class StockTopicTest {

    public static void main(String[] args) throws Exception {
        ZooKeeperClient zkClient = new ZooKeeperClient();
        zkClient.connect();

        try {
            zkClient.createTopicMetadata("stock-prices", 1);
            zkClient.storePartitionMetadata(
                    "stock-prices", 0, 0, new ArrayList<>()
            );
            System.out.println("Topic exists: " + zkClient.topicExists("stock-prices"));

            System.out.println("Created topic: stock-prices");
        } finally {
            zkClient.close();
        }
    }
}
