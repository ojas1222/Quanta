package com.quanta.broker;

public class ZooKeeperTest {

    public static void main(String[] args) throws Exception {

        int brokerId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);

        ZooKeeperClient client = new ZooKeeperClient();

        client.connect();

        client.watchBrokers();

        BrokerInfo broker = new BrokerInfo(
                brokerId,
                "localhost",
                port
        );

        client.registerBroker(broker);

        client.electController(broker);

        client.electController(broker);
        client.watchController(broker);

        Thread.sleep(20000);

        client.close();
    }
}