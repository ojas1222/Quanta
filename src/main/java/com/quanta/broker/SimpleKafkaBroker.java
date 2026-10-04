//defines the broker structure.

package com.quanta.broker;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.concurrent.atomic.AtomicBoolean;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class SimpleKafkaBroker {

    private final int brokerId;
    private final String host;
    private final int port;

    private final Map<String, Map<Integer, Partition>> topics;

    private final ExecutorService executor;

    private ServerSocketChannel serverChannel;

    private final AtomicBoolean running;
    private final AtomicBoolean isController;

    private final Map<Integer, BrokerInfo> clusterMetadata;

    private final String dataDir;

    private final ZooKeeperClient zkClient;

    public SimpleKafkaBroker(int brokerId,String host,int port,int zkPort) {
    this.brokerId = brokerId;
    this.host = host;
    this.port = port;

    this.dataDir = "data" + java.io.File.separator + brokerId;

    java.io.File dataDirectory =
            new java.io.File(this.dataDir);

    if (!dataDirectory.exists()) {
        dataDirectory.mkdirs();
    }

    this.topics = new java.util.concurrent.ConcurrentHashMap<>();
    this.clusterMetadata = new java.util.concurrent.ConcurrentHashMap<>();

    this.executor = Executors.newFixedThreadPool(10);

    try {
        this.serverChannel = ServerSocketChannel.open();
    } catch (IOException e) {
        throw new RuntimeException(
                "Failed to open server channel", e
        );
    }

    this.zkClient = new ZooKeeperClient();

    this.running = new AtomicBoolean(false);
    this.isController = new AtomicBoolean(false);
    }

    public void start() throws IOException {
        if (running.get()) {
            return;
        }

        serverChannel.bind(
                new java.net.InetSocketAddress(host, port)
        );

        running.set(true);

        try {
            BrokerInfo brokerInfo =new BrokerInfo(brokerId, host, port);

            registerWithZookeeper(brokerInfo);
            electController(brokerInfo);

            zkClient.watchController(brokerInfo);
            loadExistingTopics();
            acceptConnections();    
            System.out.println("Broker " + brokerId +" started on " + host + ":" + port);

        } catch (Exception e) {
            running.set(false);

            try {
                serverChannel.close();
            } catch (IOException ignored) {
            }

            throw new IOException(
                    "Failed to start broker", e
            );
        }
    }

    private void registerWithZookeeper(BrokerInfo brokerInfo) throws Exception {
        zkClient.connect();
        zkClient.registerBroker(brokerInfo);
        zkClient.watchBrokers( this::onBrokersChanged);
    }

    private void onBrokersChanged(java.util.List<BrokerInfo> brokers) {
        clusterMetadata.clear();
        for (BrokerInfo broker : brokers) {
            clusterMetadata.put(broker.getId(), broker);
        }

        System.out.println("Updated cluster metadata: " + clusterMetadata);

        if (isController.get()) {
            try {
                rebalancePartitions();
            } catch (Exception e) {
                System.err.println("Failed to rebalance partitions: " + e.getMessage());
            }
        }
    }

    public void stop() {
        if (!running.get()) {
            return;
        }

        running.set(false);

        try {
            if (serverChannel != null && serverChannel.isOpen()) {
                serverChannel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        executor.shutdown();

        try {
            zkClient.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("Broker " + brokerId + " stopped.");
    }

    private void electController(BrokerInfo brokerInfo) throws Exception {

        boolean elected =zkClient.electController(brokerInfo);

        isController.set(elected);

        if (elected) {
            System.out.println("Broker " + brokerId +" is the controller.");
        } else {
            System.out.println(
                    "Broker " + brokerId +
                    " is not the controller."
            );
        }
    }

    private void createTopic(String topic,int numPartitions,short replicationFactor) throws Exception {

        if (!isController.get()) {
            throw new IllegalStateException(
                    "Only the controller can create topics"
            );
        }

        if (topics.containsKey(topic)) {
            throw new IllegalArgumentException(
                    "Topic already exists: " + topic
            );
        }

        //create new topic metadata in ZooKeeper
        zkClient.createTopicMetadata(
                topic,
                numPartitions
        );

        java.io.File topicDir =new java.io.File(dataDir+ java.io.File.separator+ topic
        );

        if (!topicDir.exists() && !topicDir.mkdirs()) {
            throw new IOException("Failed to create topic directory: " + topic);
        }

        //create partitions
        Map<Integer, Partition> partitions =
                new java.util.concurrent.ConcurrentHashMap<>();

        for (int i = 0; i < numPartitions; i++) {

            //assuming current broker is the leader
            int leader = brokerId;

            java.util.List<Integer> followers =
                    new java.util.ArrayList<>();

            Partition partition =new Partition(i,leader,followers,topicDir.getAbsolutePath()+ java.io.File.separator+ "partition-" + i
            );

            partitions.put(i, partition);

            // Store partition metadata in ZooKeeper
            zkClient.storePartitionMetadata(topic,i,leader,followers );
        }

        topics.put(topic, partitions); //add topic to local brokers metadata

        System.out.println("Created topic '" + topic+ "' with "+ numPartitions+ " partitions."
        );
    }

    private void loadTopic(String topic) throws Exception {
        if (topics.containsKey(topic)) {
            return;
        }

        if (!zkClient.topicExists(topic)) {
            throw new IllegalArgumentException("Topic does not exist: " + topic);
        }

        java.io.File topicDir = new java.io.File(dataDir + java.io.File.separator + topic);

        if (!topicDir.exists() && !topicDir.mkdirs()) {
            throw new IOException("Failed to create topic directory: " + topic);
        }

        Map<Integer, Partition> partitions = new java.util.concurrent.ConcurrentHashMap<>();

        List<String> partitionNodes = zkClient.getTopicPartitions(topic);

        for (String partitionNode : partitionNodes) {
            int partitionId = Integer.parseInt(partitionNode.substring("partition-".length()));

            String metadata = zkClient.getPartitionMetadata(topic, partitionId);

            String[] parts = metadata.split(":", 2);

            int leader = Integer.parseInt(parts[0]);

            List<Integer> followers = new java.util.ArrayList<>();

            if (parts.length > 1 && !parts[1].isEmpty()) {
                String[] followerIds = parts[1].split(",");

                for (String followerId : followerIds) {
                    followers.add(Integer.parseInt(followerId));
                }
            }

            Partition partition = new Partition(partitionId,leader,followers,topicDir.getAbsolutePath() + java.io.File.separator + "partition-" + partitionId
            );

            partitions.put(partitionId, partition);
        }

        topics.put(topic, partitions);

        System.out.println("Loaded topic '" + topic + "' with " + partitions.size() + " partitions.");
    }

    private void loadExistingTopics() throws Exception {
        List<String> topicNames = zkClient.getTopics();

        for (String topic : topicNames) {
            loadTopic(topic);
        }
    }

    private void rebalancePartitions() throws Exception {
        if (!isController.get()) {
            return;
        }

        List<Integer> brokerIds = new java.util.ArrayList<>(clusterMetadata.keySet());

        if (brokerIds.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Map<Integer, Partition>> topicEntry : topics.entrySet()) {
            String topic = topicEntry.getKey();

            for (Partition partition : topicEntry.getValue().values()) {
                int leader = partition.getLeader();

                if (!clusterMetadata.containsKey(leader)) {
                    int newLeader = brokerIds.get(0);
                    partition.setLeader(newLeader);
                    leader = newLeader;
                }

                List<Integer> followers = new java.util.ArrayList<>();

                for (Integer brokerId : brokerIds) {
                    if (brokerId != leader) {
                        followers.add(brokerId);
                    }
                }

                partition.setFollowers(followers);
                zkClient.storePartitionMetadata(topic, partition.getId(), leader, followers);
            }
        }

        System.out.println("Partition rebalancing completed.");
    }

    private Partition getPartitionForProduce(String topic) {
        Map<Integer, Partition> partitions = topics.get(topic);

        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalArgumentException("Topic does not exist: " + topic);
        }

        return partitions.values().iterator().next();
    }

    private byte[] handleProduceRequest(byte[] data) {
        try {
            String[] request = Protocol.decodeProduceRequest(data);
            String topic = request[0];
            String message = request[1];

            Partition partition = getPartitionForProduce(topic);
            byte[] messageBytes = message.getBytes();
            long offset = partition.append(messageBytes);

            replicateToFollowers(topic, partition, messageBytes, offset);

            return Protocol.encodeProduceResponse(offset);
        } catch (Exception e) {
            System.err.println("Failed to handle produce request: " + e.getMessage());
            return null;
        }
    }

    private byte[] handleFetchRequest(byte[] data) {
        try {
            String[] request = Protocol.decodeFetchRequest(data);
            String topic = request[0];
            long offset = Long.parseLong(request[1]);

            Partition partition = getPartitionForProduce(topic);
            List<byte[]> messages = partition.readMessages(offset, 1024);

            if (messages.isEmpty()) {
                return Protocol.encodeFetchResponse("");
            }

            String message = new String(messages.get(0));
            return Protocol.encodeFetchResponse(message);
        } catch (Exception e) {
            System.err.println("Failed to handle fetch request: " + e.getMessage());
            return null;
        }
    }

    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        for (Integer followerId : partition.getFollowers()) {
            BrokerInfo follower = clusterMetadata.get(followerId);

            if (follower == null) {
                continue;
            }

            try (SocketChannel channel = SocketChannel.open()) {
                channel.connect(new java.net.InetSocketAddress(follower.getHost(), follower.getPort()));

                byte[] request = Protocol.encodeReplicateRequest(
                        topic,
                        partition.getId(),
                        offset,
                        message
                );

                channel.write(ByteBuffer.wrap(request));

                ByteBuffer response = ByteBuffer.allocate(9);
                while (response.hasRemaining()) {
                    channel.read(response);
                }

                response.flip();
                response.get();
                long acknowledgedOffset = response.getLong();

                System.out.println("Replicated offset " + acknowledgedOffset +
                        " to broker " + followerId);

            } catch (Exception e) {
                System.err.println("Failed to replicate to broker " +
                        followerId + ": " + e.getMessage());
            }
        }
    }

    private void handleReplicateRequest(SocketChannel clientChannel, byte[] data) throws IOException {
        try {
            Object[] request = Protocol.decodeReplicateRequest(data);

            String topic = (String) request[0];
            int partitionId = (Integer) request[1];
            long offset = (Long) request[2];
            byte[] message = (byte[]) request[3];

            Map<Integer, Partition> partitions = topics.get(topic);
            if (partitions == null || !partitions.containsKey(partitionId)) {
                throw new IllegalArgumentException("Topic or partition does not exist");
            }

            Partition partition = partitions.get(partitionId);
            partition.append(message);

            ByteBuffer response = ByteBuffer.allocate(9);
            response.put(Protocol.PRODUCE_RESPONSE);
            response.putLong(offset);
            response.flip();

            clientChannel.write(response);
        } catch (Exception e) {
            System.err.println("Failed to handle replication request: " + e.getMessage());
        }
    }

    private void acceptConnections() {
        executor.submit(() -> {
            while (running.get()) {
                try {
                    SocketChannel client = serverChannel.accept();
                    executor.submit(() -> handleClient(client));
                } catch (IOException e) {
                    if (running.get()) {
                        System.err.println("Failed to accept client: " + e.getMessage());
                    }
                }
            }
        });
    }

    private void handleClient(SocketChannel client) {
        try (SocketChannel channel = client) {
            ByteBuffer buffer = ByteBuffer.allocate(4096);

            while (running.get() && channel.read(buffer) > 0) {
                buffer.flip();

                byte[] request = new byte[buffer.remaining()];
                buffer.get(request);
                buffer.clear();

                if (request.length == 0) {
                    continue;
                }

                byte requestType = request[0];
                byte[] response = null;

                if (requestType == Protocol.PRODUCE) {
                    response = handleProduceRequest(request);
                } else if (requestType == Protocol.FETCH) {
                    response = handleFetchRequest(request);
                }
                else if (requestType == Protocol.REPLICATE) {
                    handleReplicateRequest(channel, request);
                }

                if (response != null) {
                    channel.write(ByteBuffer.wrap(response));
                }
            }
        } catch (IOException e) {
            System.err.println("Client connection closed: " + e.getMessage());
        }
    }
}