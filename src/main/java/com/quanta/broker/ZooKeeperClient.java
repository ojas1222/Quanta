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

    public void watchBrokers() throws Exception {

    List<String> brokers = zooKeeper.getChildren(
            BROKER_PATH,
            event -> {

                if (closing) {
                    return;
                }

                System.out.println("Broker list changed!");

                try {
                    watchBrokers();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
    );

    System.out.println("Active brokers:");

    for (String broker : brokers) {

        String path = BROKER_PATH + "/" + broker;

        byte[] data = zooKeeper.getData(path, false, null);

        String brokerInfo =
                new String(data, StandardCharsets.UTF_8);

        System.out.println(
                "- " + broker + " -> " + brokerInfo
        );
    }
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
public void watchController(BrokerInfo brokerInfo)
        throws Exception {

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
}