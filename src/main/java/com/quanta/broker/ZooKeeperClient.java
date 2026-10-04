package com.quanta.broker;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

import java.util.List;

public class ZooKeeperClient implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String BROKER_PATH = "/brokers";
    private static final String TOPIC_PATH = "/topics";

    private ZooKeeper zooKeeper;

    private boolean closing = false;    

    private CountDownLatch connectedSignal =
            new CountDownLatch(1);

    public void connect() throws IOException, InterruptedException {

        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS,3000,this);

        connectedSignal.await();

        System.out.println("Connected to ZooKeeper");
    }

    public void registerBroker(BrokerInfo brokerInfo)
            throws Exception {

        // Create /brokers if it does not exist
        if (zooKeeper.exists(BROKER_PATH, false) == null) {

            zooKeeper.create(
                    BROKER_PATH,
                    new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT
            );
        }

        String brokerPath = BROKER_PATH + "/broker-" + brokerInfo.getId();

        String brokerData = brokerInfo.getHost() + ":" + brokerInfo.getPort();

        // Register broker as an ephemeral node
        zooKeeper.create(
                brokerPath,
                brokerData.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL
        );

        System.out.println("Registered broker: " + brokerPath);
    }

    @Override
    public void process(WatchedEvent event) {

        if (event.getState() ==
                Event.KeeperState.SyncConnected) {

            connectedSignal.countDown();
        }
    }

    public void close() throws InterruptedException {

    closing = true;

    if (zooKeeper != null) {
        zooKeeper.close();
    }
}

//watches broker and passes on info to SimpleKafkaBroker.
public void watchBrokers(java.util.function.Consumer<List<BrokerInfo>> onChange) throws Exception {
    List<String> brokers = zooKeeper.getChildren(
            BROKER_PATH,
            event -> {

                if (closing) {
                    return;
                }

                System.out.println("Broker list changed!");

                try {
                    watchBrokers(onChange);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
    );

    List<BrokerInfo> brokerInfos = new java.util.ArrayList<>();

    for (String broker : brokers) {

        String path = BROKER_PATH + "/" + broker;

        byte[] data =
                zooKeeper.getData(path, false, null);

        String brokerInfo =
                new String(data, StandardCharsets.UTF_8);

        String[] parts = brokerInfo.split(":");

        int brokerId =
                Integer.parseInt(
                        broker.substring("broker-".length())
                );

        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        brokerInfos.add(
                new BrokerInfo(
                        brokerId,
                        host,
                        port
                )
        );
    }

    onChange.accept(brokerInfos);
}

//Overloading implementation for ZooKeeper test.
//NOT relevant to actual implementation.
public void watchBrokers() throws Exception {
    watchBrokers(brokers -> {
            System.out.println("Active brokers:");
            for (BrokerInfo broker : brokers) {
                System.out.println("- broker-" + broker.getId()+ " -> "
                + broker.getHost()+ ":"+ broker.getPort());
            }
        });
    }

//simple controller election.
public boolean electController(BrokerInfo brokerInfo)
        throws Exception {

    String controllerPath = "/controller";

    String data =
            String.valueOf(brokerInfo.getId());

    try {

        zooKeeper.create(
                controllerPath,
                data.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL
        );

        System.out.println("Broker " + brokerInfo.getId()+ " became controller");

        return true;

    } catch (org.apache.zookeeper.KeeperException.NodeExistsException e) {

        System.out.println("Broker " + brokerInfo.getId()+ " is not the controller");

        return false;
    }
}

    //watch controller for failiure + re-election.
    public void watchController(BrokerInfo brokerInfo)throws Exception {

    zooKeeper.exists(
            "/controller",
            event -> {

                if (closing) {
                    return;
                }

                if (event.getType() ==
                        Watcher.Event.EventType.NodeDeleted) {

                    System.out.println(
                            "Controller has failed. Starting election..."
                    );

                    try {
                        electController(brokerInfo);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                if (!closing) {
                    try {
                        watchController(brokerInfo);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        );
    }

//handling the META-DATA for SimpleKafkaBroker
    public void createTopicMetadata(String topic,int numPartitions) throws Exception {
    // Create /topics if it does not exist
        if (zooKeeper.exists(TOPIC_PATH, false) == null) {
            zooKeeper.create(
                    TOPIC_PATH,
                    new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT
            );
        }
        String topicPath =TOPIC_PATH + "/" + topic;

        //create th topic node
        if (zooKeeper.exists(topicPath, false) == null) {
            zooKeeper.create(
                    topicPath,
                    new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT
            );
        }

        for (int i = 0; i < numPartitions; i++) {

            String partitionPath =topicPath + "/partition-" + i;

            if (zooKeeper.exists(partitionPath, false) == null) {
                zooKeeper.create(
                        partitionPath,
                        new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT
                );
            }
        }
    }

    public void storePartitionMetadata(String topic,int partitionId,int leader,List<Integer> followers) throws Exception {
        String path =TOPIC_PATH+ "/" + topic+ "/partition-" + partitionId;

        StringBuilder data =new StringBuilder();

        data.append(leader);
        data.append(":");

        for (int i = 0; i < followers.size(); i++) {
            if (i > 0) {
                data.append(",");
            }

            data.append(followers.get(i));
        }

        zooKeeper.setData(path,data.toString().getBytes(StandardCharsets.UTF_8),-1);
    }

    public String getPartitionMetadata(String topic,int partitionId) throws Exception {
        String path =TOPIC_PATH+ "/" + topic+ "/partition-" + partitionId;

        byte[] data =zooKeeper.getData(path,false,null);
        return new String(data, StandardCharsets.UTF_8);
    }

    public boolean topicExists(String topic) throws Exception {
        return zooKeeper.exists(TOPIC_PATH + "/" + topic, false) != null;
    }   

    public List<String> getTopicPartitions(String topic) throws Exception {
        return zooKeeper.getChildren(TOPIC_PATH + "/" + topic, false);
    }

    public List<String> getTopics() throws Exception {
        if (zooKeeper.exists(TOPIC_PATH, false) == null) {
            return new java.util.ArrayList<>();
        }

        return zooKeeper.getChildren(TOPIC_PATH, false);
    }
}