package com.quanta.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class BrokerTest {

    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("localhost", 9092);

        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());

        // PRODUCE
        byte[] produceRequest = Protocol.encodeProduceRequest("test-topic", "Hello Quanta!");
        out.write(produceRequest);
        out.flush();

        byte[] produceResponse = new byte[9];
        in.readFully(produceResponse);

        long offset = Protocol.decodeProduceResponse(produceResponse);
        System.out.println("Produced message at offset: " + offset);

        // FETCH
        byte[] fetchRequest = Protocol.encodeFetchRequest("test-topic", offset);
        out.write(fetchRequest);
        out.flush();

        byte responseType = in.readByte();
        int messageLength = in.readInt();

        byte[] messageBytes = new byte[messageLength];
        in.readFully(messageBytes);

        String message = new String(messageBytes);

        System.out.println("Fetched message: " + message);

        socket.close();
    }
}