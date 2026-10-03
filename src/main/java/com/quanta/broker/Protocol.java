package com.quanta.broker;
import java.nio.ByteBuffer;

public class Protocol {

    // client request type
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // broker response type
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;

//client to broker request 
/*
Quanta emphasises a binary protocol for efficiency.
Where each message is identified by a unique byte-code followed by the actual message payload.
*/
public static byte[] encodeProduceRequest(String topic, String message) {
    byte[] topicBytes = topic.getBytes(); 
    byte[] messageBytes = message.getBytes();

    ByteBuffer buffer = ByteBuffer.allocate(
            1 + 4 + topicBytes.length + 4 + messageBytes.length
    );

    buffer.put(PRODUCE);
    buffer.putInt(topicBytes.length);
    buffer.put(topicBytes);
    buffer.putInt(messageBytes.length);
    buffer.put(messageBytes);

    return buffer.array();
}
 //decoding msg 
public static String[] decodeProduceRequest(byte[] data) {
    ByteBuffer buffer = ByteBuffer.wrap(data);

    buffer.get(); // skip request type

    int topicLength = buffer.getInt();
    byte[] topicBytes = new byte[topicLength];
    buffer.get(topicBytes);

    int messageLength = buffer.getInt();
    byte[] messageBytes = new byte[messageLength];
    buffer.get(messageBytes);

    return new String[]{
            new String(topicBytes),
            new String(messageBytes)
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
public static byte[] encodeFetchRequest(String topic, long offset) {
    byte[] topicBytes = topic.getBytes();

    ByteBuffer buffer = ByteBuffer.allocate(
            1 + 4 + topicBytes.length + 8
    );

    buffer.put(FETCH);
    buffer.putInt(topicBytes.length);
    buffer.put(topicBytes);
    buffer.putLong(offset);

    return buffer.array();
}
public static String[] decodeFetchRequest(byte[] data) {
    ByteBuffer buffer = ByteBuffer.wrap(data);

    buffer.get(); // skip request type

    int topicLength = buffer.getInt();
    byte[] topicBytes = new byte[topicLength];
    buffer.get(topicBytes);

    long offset = buffer.getLong();

    return new String[]{
            new String(topicBytes),
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
}