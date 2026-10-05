package com.quanta.broker;
import java.nio.ByteBuffer;

public class Protocol {

    // client request type
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
    public static final byte REPLICATE = 0x05;
    // broker response type
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;

//::WIRE PROTOCOLS::
/*
Quanta emphasises a binary protocol for efficiency.
Where each message is identified by a unique byte-code followed by the actual message payload.
*/
public static byte[] encodeProduceRequest(String topic, int partition, String message) {
    byte[] topicBytes = topic.getBytes();
    byte[] messageBytes = message.getBytes();

    ByteBuffer buffer = ByteBuffer.allocate(
            1 + 4 + topicBytes.length + 4 + 4 + messageBytes.length
    );

    buffer.put(PRODUCE);
    buffer.putInt(topicBytes.length);
    buffer.put(topicBytes);
    buffer.putInt(partition);
    buffer.putInt(messageBytes.length);
    buffer.put(messageBytes);

    return buffer.array();
}
//overloaded for brokerTest.
public static byte[] encodeProduceRequest(String topic, String message) {
    return encodeProduceRequest(topic, 0, message);
}
 //decoding msg 
public static String[] decodeProduceRequest(byte[] data) {
    ByteBuffer buffer = ByteBuffer.wrap(data);

    buffer.get(); // skip request type

    int topicLength = buffer.getInt();
    byte[] topicBytes = new byte[topicLength];
    buffer.get(topicBytes);

    int partition = buffer.getInt();

    int messageLength = buffer.getInt();
    byte[] messageBytes = new byte[messageLength];
    buffer.get(messageBytes);

    return new String[]{new String(topicBytes),String.valueOf(partition), new String(messageBytes)
    };
}

//handling producer response encoding and decoding.
public static byte[] encodeProduceResponse(long offset) {
    ByteBuffer buffer = ByteBuffer.allocate(1 + 8);

    buffer.put(PRODUCE_RESPONSE);
    buffer.putLong(offset);

    return buffer.array();
}

public static long decodeProduceResponse(byte[] data) {
    ByteBuffer buffer = ByteBuffer.wrap(data);

    buffer.get(); // skip response type

    return buffer.getLong();
}

//fetching request protocol.
public static byte[] encodeFetchRequest(String topic, int partition, long offset) {
    byte[] topicBytes = topic.getBytes();

    ByteBuffer buffer = ByteBuffer.allocate(
            1 + 4 + topicBytes.length + 4 + 8
    );

    buffer.put(FETCH);
    buffer.putInt(topicBytes.length);
    buffer.put(topicBytes);
    buffer.putInt(partition);
    buffer.putLong(offset);

    return buffer.array();
}

public static byte[] encodeFetchRequest(String topic, long offset) {
    return encodeFetchRequest(topic, 0, offset);
}

public static String[] decodeFetchRequest(byte[] data) {
    ByteBuffer buffer = ByteBuffer.wrap(data);

    buffer.get(); // skip request type

    int topicLength = buffer.getInt();
    byte[] topicBytes = new byte[topicLength];
    buffer.get(topicBytes);

    int partition = buffer.getInt();
    long offset = buffer.getLong();

    return new String[]{
            new String(topicBytes),
            String.valueOf(partition),
            String.valueOf(offset)
    };
}
//fetching response protocol.
public static byte[] encodeFetchResponse(String message) {
    byte[] messageBytes = message.getBytes();

    ByteBuffer buffer = ByteBuffer.allocate(
            1 + 4 + messageBytes.length
    );

    buffer.put(FETCH_RESPONSE);
    buffer.putInt(messageBytes.length);
    buffer.put(messageBytes);

    return buffer.array();
}
public static String decodeFetchResponse(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        buffer.get(); // skip response type

        int messageLength = buffer.getInt();
        byte[] messageBytes = new byte[messageLength];
        buffer.get(messageBytes);

        return new String(messageBytes);
    }

    //handling message replication.
    public static byte[] encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        byte[] topicBytes = topic.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + topicBytes.length + 4 + 8 + 4 + message.length);

        buffer.put(REPLICATE);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);

        return buffer.array();
    }

    // metadata request
    public static byte[] encodeMetadataRequest(String topic) {
        byte[] topicBytes = topic.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(
                1 + 4 + topicBytes.length
        );

        buffer.put(METADATA);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);

        return buffer.array();
    }

    public static String decodeMetadataRequest(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        buffer.get(); // skip request type

        int topicLength = buffer.getInt();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);

        return new String(topicBytes);
    }

    // metadata response
    public static byte[] encodeMetadataResponse(String metadata) {
        byte[] metadataBytes = metadata.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(
                1 + 4 + metadataBytes.length
        );

        buffer.put(METADATA);
        buffer.putInt(metadataBytes.length);
        buffer.put(metadataBytes);

        return buffer.array();
    }

    public static String decodeMetadataResponse(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        buffer.get(); // skip response type

        int metadataLength = buffer.getInt();
        byte[] metadataBytes = new byte[metadataLength];
        buffer.get(metadataBytes);

        return new String(metadataBytes);
    }

    public static Object[] decodeReplicateRequest(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        buffer.get();
        int topicLength = buffer.getInt();

        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);

        String topic = new String(topicBytes);
        int partition = buffer.getInt();
        long offset = buffer.getLong();

        int messageLength = buffer.getInt();
        byte[] message = new byte[messageLength];
        buffer.get(message);

        return new Object[]{topic, partition, offset, message};
    }
}