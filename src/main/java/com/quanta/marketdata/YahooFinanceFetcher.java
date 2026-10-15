
package com.quanta.marketdata;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

public class YahooFinanceFetcher {

    private static final HttpClient CLIENT =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    public static double fetchPrice(String symbol)
            throws Exception {

        String url ="https://query1.finance.yahoo.com/v8/finance/chart/"+ symbol + "?range=1d&interval=1m";

        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(20)).header("User-Agent", "Mozilla/5.0")
                .GET().build();

        HttpResponse<String> response = CLIENT.send(request,HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) { throw new RuntimeException("Yahoo Finance returned HTTP "+ response.statusCode()
            );
        }

        String json = response.body();
        String marker = "\"regularMarketPrice\":";
        int index = json.indexOf(marker);

        if (index == -1) {
            throw new RuntimeException("Could not find regularMarketPrice in response");
        }

        int start = index + marker.length();
        int end = start;

        while (end < json.length()&& (Character.isDigit(json.charAt(end))|| json.charAt(end) == '.'
                || json.charAt(end) == '-')) {
            end++;
        }

        if (start == end) {
            throw new RuntimeException("Invalid stock price returned by Yahoo Finance");
        }

        return Double.parseDouble(json.substring(start, end));
    }

    public static void main(String[] args) throws Exception {
        String symbol = "MSFT";
        double price = fetchPrice(symbol);

        System.out.println("Symbol: " + symbol);
        System.out.println("Price: " + price);
        System.out.println("Fetched at: " + Instant.now());
    }
}
