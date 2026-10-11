
package com.quanta.client;
import com.quanta.broker.Protocol;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
public class FollowerFetchTest {
    public static void main(String[] args) throws Exception {
        for (long offset = 5; offset <= 5; offset++) {
            try (SocketChannel channel = SocketChannel.open()) {
                channel.connect(new InetSocketAddress("localhost", 9092));
                channel.write(ByteBuffer.wrap(Protocol.encodeFetchRequest("test-topic", 0, offset)));
                ByteBuffer header = ByteBuffer.allocate(5);
                while (header.hasRemaining()) {
                    if (channel.read(header) == -1) throw new java.io.IOException("Broker closed connection");
                }
                header.flip();
                byte type = header.get();
                int length = header.getInt();
                if (type != Protocol.FETCH_RESPONSE) throw new java.io.IOException("Unexpected response type: " + type);
                ByteBuffer body = ByteBuffer.allocate(length);
                while (body.hasRemaining()) {
                    if (channel.read(body) == -1) throw new java.io.IOException("Broker closed before message body");
                }
                byte[] message = body.array();
                System.out.println("Broker--0s Offset " + offset + " (length " + length + "): " + new String(message));
            }
        }
    }
}
