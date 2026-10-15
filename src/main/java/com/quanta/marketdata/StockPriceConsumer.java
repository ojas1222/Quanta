
package com.quanta.marketdata;

import com.quanta.client.SimpleKafkaConsumer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class StockPriceConsumer {

    public static void main(String[] args) throws Exception {

        SimpleKafkaConsumer consumer =
                new SimpleKafkaConsumer(
                        "localhost", 9092, "stock-prices", 0
                );

        consumer.initialize();
        consumer.seek(5);

        List<byte[]> messages = consumer.poll();

        if (messages.isEmpty()) {
            System.out.println("No stock-price messages found.");
        } else {
            for (byte[] message : messages) {
                String data =
                        new String(message, StandardCharsets.UTF_8);

                System.out.println("Consumed stock price: " + data);
            }
        }
    }
}
