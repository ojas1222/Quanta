
package com.quanta.marketdata;

import com.quanta.client.SimpleKafkaConsumer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class StockReturnConsumer {
    public static void main(String[] args) throws Exception {
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost", 9092, "stock-prices", 0);
        Map<String, Double> previousPrices = new HashMap<>();

        consumer.initialize();
        consumer.seek(0);
        System.out.println("Stock return consumer started.");

        try {
            while (true) {
                var messages = consumer.poll();

                if (messages.isEmpty()) {
                    Thread.sleep(500);
                    continue;
                }

                for (byte[] rawMessage : messages) {
                    String message = new String(rawMessage, StandardCharsets.UTF_8);
                    String[] parts = message.split(",");

                    if (parts.length != 3) {
                        System.err.println("Skipping invalid message: " + message);
                        continue;
                    }

                    String symbol = parts[0];

                    try {
                        double price = Double.parseDouble(parts[2]);

                        if (price <= 0) {
                            System.err.println("Skipping invalid price: " + message);
                            continue;
                        }

                        Double previousPrice = previousPrices.get(symbol);

                        if (previousPrice == null) {
                            System.out.println(symbol + " | First price: " + price);
                        } else {
                            double percentageReturn = ((price - previousPrice) / previousPrice) * 100.0;
                            System.out.printf("%s | Previous: %.3f | Current: %.3f | Return percentage: %.4f%%%n",
                                    symbol, previousPrice, price, percentageReturn);
                        }

                        previousPrices.put(symbol, price);
                    } catch (NumberFormatException e) {
                        System.err.println("Skipping invalid price: " + message);
                    }
                }
            }
        } finally {
            //this stops the consumer when the application exits.
            consumer.stopConsuming();
        }
    }
}
