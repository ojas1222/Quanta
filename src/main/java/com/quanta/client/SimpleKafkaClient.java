package com.quanta.client;

import com.quanta.broker.Protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleKafkaClient {

    private final String bootstrapBroker;
    private final int bootstrapPort;
    private final Map<String, TopicMetadata> topicMetadata;

    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.topicMetadata = new ConcurrentHashMap<>();
    }

    public void initialize(String topic) throws IOException {
        refreshMetadata(topic);
    }

//client ask broker for metadata for storage
    public void refreshMetadata(String topic) throws IOException {
        // String topic = topicMetadata.keySet().stream().findFirst().orElse(null);

        // if (topic == null) {
        //     return;
        // }

        byte[] request = Protocol.encodeMetadataRequest(topic);

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(bootstrapBroker, bootstrapPort));
            channel.write(ByteBuffer.wrap(request));

            ByteBuffer header = ByteBuffer.allocate(5);

            while (header.hasRemaining()) {
                if (channel.read(header) == -1) {
                    throw new IOException("Broker closed connection before sending metadata response");
                }
            }

            header.flip();

            byte responseType = header.get();

            if (responseType != Protocol.METADATA) {
                throw new IOException("Unexpected response type: " + responseType);
            }

            int metadataLength = header.getInt();

            ByteBuffer metadataBuffer = ByteBuffer.allocate(metadataLength);

            while (metadataBuffer.hasRemaining()) {
                if (channel.read(metadataBuffer) == -1) {
                    throw new IOException("Broker closed connection before sending metadata");
                }
            }

            metadataBuffer.flip();

            byte[] metadataBytes = new byte[metadataLength];
            metadataBuffer.get(metadataBytes);

            String metadataString = new String(metadataBytes);

            TopicMetadata metadata = new TopicMetadata();

            if (!metadataString.isEmpty()) {
                String[] partitions = metadataString.split(";");

                for (String partitionData : partitions) {
                    String[] parts = partitionData.split(":");

                    int partitionId = Integer.parseInt(parts[0]);
                    int leader = Integer.parseInt(parts[1]);
                    String leaderHost = parts[2];
                    int leaderPort = Integer.parseInt(parts[3]);

                    List<Integer> followers = new ArrayList<>();

                    if (parts.length > 4 && !parts[4].isEmpty()) {
                        String[] followerIds = parts[4].split(",");

                        for (String followerId : followerIds) {
                            followers.add(Integer.parseInt(followerId));
                        }
                    }
                    metadata.partitions.put(partitionId,new PartitionMetadata(
                                partitionId,
                                leader,
                                leaderHost,
                                leaderPort,
                                followers
                        )
                    );
                }
            }

            topicMetadata.put(topic, metadata);
            System.out.println("Client metadata for " + topic + ": " + metadataString);
        }
    }

    //connect broker to specific host or port.
    private static class BrokerAddress {
        private final String host;
        private final int port;

        public BrokerAddress(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    private BrokerAddress getBrokerForPartition(String topic, int partition) throws IOException {
        TopicMetadata metadata = topicMetadata.get(topic);

        if (metadata == null) {
            throw new IOException("No metadata found for topic: " + topic);
        }

        PartitionMetadata partitionMetadata = metadata.partitions.get(partition);

        if (partitionMetadata == null) {
            throw new IOException("No metadata found for partition: " + partition);
        }

        return new BrokerAddress(
                partitionMetadata.leaderHost,
                partitionMetadata.leaderPort
        );
    }


    public long send(String topic, int partition, byte[] message) throws IOException {
        byte[] request = Protocol.encodeProduceRequest(topic,partition, new String(message));

        BrokerAddress broker = getBrokerForPartition(topic, partition);

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(broker.host, broker.port));
            channel.write(ByteBuffer.wrap(request));

            ByteBuffer response = ByteBuffer.allocate(9);

            while (response.hasRemaining()) {
                if (channel.read(response) == -1) {
                    throw new IOException("Broker closed connection before sending response");
                }
            }

            response.flip();

            byte responseType = response.get();

            if (responseType != Protocol.PRODUCE_RESPONSE) {
                throw new IOException("Unexpected response type: " + responseType);
            }

            return response.getLong();
        }
    }

    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        byte[] request = Protocol.encodeFetchRequest(topic,partition,offset);

        BrokerAddress broker = getBrokerForPartition(topic, partition);

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(broker.host, broker.port));
            channel.write(ByteBuffer.wrap(request));

            ByteBuffer header = ByteBuffer.allocate(5);

            while (header.hasRemaining()) {
                if (channel.read(header) == -1) {
                    throw new IOException("Broker closed connection before sending response");
                }
            }

            header.flip();

            byte responseType = header.get();

            if (responseType != Protocol.FETCH_RESPONSE) {
                throw new IOException("Unexpected response type: " + responseType);
            }

            int messageLength = header.getInt();

            if (messageLength == 0) {
                return new ArrayList<>();
            }

            if (messageLength > maxBytes) {
                throw new IOException("Message exceeds maximum fetch size");
            }

            ByteBuffer messageBuffer = ByteBuffer.allocate(messageLength);

            while (messageBuffer.hasRemaining()) {
                if (channel.read(messageBuffer) == -1) {
                    throw new IOException("Broker closed connection before sending message");
                }
            }

            return List.of(messageBuffer.array());
        }
    }

    private static class TopicMetadata {
        private final Map<Integer, PartitionMetadata> partitions;

        public TopicMetadata() {
            this.partitions = new ConcurrentHashMap<>();
        }
    }

    private static class PartitionMetadata {
        private final int partitionId;
        private final int leader;
        private final String leaderHost;
        private final int leaderPort;
        private final List<Integer> followers;

        public PartitionMetadata(int partitionId, int leader, String leaderHost, int leaderPort, List<Integer> followers) {
            this.partitionId = partitionId;
            this.leader = leader;
            this.leaderHost = leaderHost;
            this.leaderPort = leaderPort;
            this.followers = followers;
        }
    }
}