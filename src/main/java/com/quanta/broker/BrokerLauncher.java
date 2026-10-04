//code to launch brokers for BrokerTest.

package com.quanta.broker;

public class BrokerLauncher {

    public static void main(String[] args) throws Exception {
        SimpleKafkaBroker broker = new SimpleKafkaBroker(0, "localhost", 9092, 2181);
        broker.start();
    }
}