//second brokerlauncher to test message replication

package com.quanta.broker;

public class Broker2Launcher {
    public static void main(String[] args) throws Exception {
        SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "localhost", 9093, 2181);
        broker.start();
    }
}