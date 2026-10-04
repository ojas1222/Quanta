package com.quanta.broker;

import java.util.ArrayList;
import java.util.List;

public class PartitionTest {

    public static void main(String[] args) {

        String baseDir = "data/multi-segment-recovery";

        // Create enough data to produce multiple segments
        Partition partition1 = new Partition(0, 1, new ArrayList<>(), baseDir);

        for (int i = 0; i < 6000; i++) {

            byte[] message = new byte[200];

            String text = "Message-" + i;
            byte[] textBytes = text.getBytes();

            System.arraycopy( textBytes, 0, message, 0, textBytes.length);

            partition1.append(message);
        }

        System.out.println("Initial messages written: 6000");

        Partition partition2 = new Partition(0, 1, new ArrayList<>(), baseDir);

        long newOffset = partition2.append("After restart".getBytes());

        System.out.println("Offset after restart: " + newOffset);

        List<byte[]> messages = partition2.readMessages(5138, 1024);

        System.out.println("\nMessages around boundary:");

        for (byte[] message : messages) {
            System.out.println(new String(message).trim());
        }
    }
}