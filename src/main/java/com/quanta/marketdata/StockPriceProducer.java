
package com.quanta.marketdata;

import com.quanta.client.SimpleKafkaProducer;
import java.time.Instant;

public class StockPriceProducer {

    public static void main(String[] args) throws Exception {

        String topic = "stock-prices";
        String brokerHost = "localhost";
        int brokerPort = 9092;
        String symbol = "MSFT";

        SimpleKafkaProducer producer =
                new SimpleKafkaProducer(
                        brokerHost, brokerPort, topic
                );

        producer.initialize();

        try {
            while (true) {
                try {
                    double price =
                            YahooFinanceFetcher.fetchPrice(symbol);

                    String timestamp = Instant.now().toString();

                    String message =
                            symbol + "," + timestamp + "," + price;

                    long offset = producer.send(message);

                    System.out.println(
                            "Published: " + message
                            + " | Offset: " + offset
                    );

                } catch (Exception e) {
                    System.err.println(
                            "Failed to fetch or publish price: "
                            + e.getMessage()
                    );
                }

                Thread.sleep(30_000);
            }
        } finally {
            producer.close();
        }
    }
}
