package com.quanta.broker;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

//for constructor
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//for append() fn
import java.io.IOException;
import java.nio.ByteBuffer;

public class Partition {

    private final int id;
    private int leader;
    private List<Integer> followers;

    private final String baseDir;

    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;

    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;

    private final List<SegmentInfo> segments;

    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
    this.id = id;
    this.leader = leader;
    this.followers = followers;
    this.baseDir = baseDir;

    this.nextOffset = new AtomicLong(0);
    this.lock = new ReentrantReadWriteLock();
    this.segments = new ArrayList<>();

    initialize();
}

private void initialize() {
    File dir = new File(baseDir);

    if (!dir.exists()) {
        dir.mkdirs();
    }

    loadSegments();

    segments.sort((a, b) ->
            Long.compare(a.baseOffset, b.baseOffset));

    if (segments.isEmpty()) {
        createNewSegment(0);
    } else {
        SegmentInfo lastSegment =
                segments.get(segments.size() - 1);

        nextOffset.set(findNextOffset(lastSegment));

        openSegmentForAppend(lastSegment);
    }
}

private void loadSegments() {
    File dir = new File(baseDir);

    File[] files = dir.listFiles((d, name) -> name.endsWith(".log"));

    if (files == null) {
        return;
    }

    for (File file : files) {
        String fileName = file.getName();

        String offsetString =
                fileName.substring(0, fileName.length() - 4);

        long baseOffset = Long.parseLong(offsetString);

        String logFile = file.getAbsolutePath();

        String indexFile =
                new File(dir, offsetString + ".index").getAbsolutePath();

        segments.add(
                new SegmentInfo(
                        baseOffset,
                        logFile,
                        indexFile
                )
        );
    }
}

public long append(byte[] message) {
    lock.writeLock().lock();

    try {
        long recordSize = 4 + message.length;

        // Create a new segment if the current one would exceed 1 MB
        if (activeLogChannel.size() + recordSize > 1024 * 1024) {
            createNewSegment(nextOffset.get());
        }

        long offset = nextOffset.get();

        ByteBuffer buffer = ByteBuffer.allocate((int) recordSize);

        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();

        long position = activeLogChannel.position();

        activeLogChannel.write(buffer);
        activeLogChannel.force(true);

        updateIndex(offset,position,segments.get(segments.size() - 1));

        nextOffset.incrementAndGet();

        return offset;

    } catch (IOException e) {
        throw new RuntimeException("Failed to append message", e);

    } finally {
        lock.writeLock().unlock();
    }
}

//read message until byte limit, if exceed then new segment.
public List<byte[]> readMessages(long offset, int maxBytes) {
    lock.readLock().lock();

    try {
        List<byte[]> messages = new ArrayList<>();
        int totalBytes = 0;

        SegmentInfo segment = findSegmentForOffset(offset);

        if (segment == null) {
            return messages;
        }

        long position = findPositionForOffset(segment, offset);
        if (position == -1) {
            return messages;
        }

        int segmentIndex = segments.indexOf(segment);

        while (segmentIndex < segments.size()
                && totalBytes < maxBytes) {

            segment = segments.get(segmentIndex);

            try (RandomAccessFile file =
                         new RandomAccessFile(segment.logFile, "r")) {

                FileChannel channel = file.getChannel();
                channel.position(position);

                while (totalBytes < maxBytes) {

                    ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

                    if (channel.read(sizeBuffer) < 4) {
                        break;
                    }

                    sizeBuffer.flip();

                    int messageSize = sizeBuffer.getInt();

                    if (totalBytes + 4 + messageSize > maxBytes) {
                        break;
                    }

                    ByteBuffer messageBuffer =
                            ByteBuffer.allocate(messageSize);

                    if (channel.read(messageBuffer) < messageSize) {
                        break;
                    }

                    messages.add(messageBuffer.array());

                    totalBytes += messageSize + 4;
                }
            }

            segmentIndex++;

            if (segmentIndex < segments.size()) {
                position = 0;
            }
        }

        return messages;

    } catch (IOException e) {
        throw new RuntimeException(
                "Failed to read messages", e);

    } finally {
        lock.readLock().unlock();
    }
}

//finding data based on base + offset.
private SegmentInfo findSegmentForOffset(long offset) {
    if (segments.isEmpty()) {
        return null;
    }

    //implement binary search.
    int low = 0;
    int high = segments.size() - 1;

    while (low <= high) {
        int mid = (low + high) / 2;

        SegmentInfo current = segments.get(mid);

        if (mid < segments.size() - 1) {
            SegmentInfo next = segments.get(mid + 1);

            if (offset >= current.baseOffset
                    && offset < next.baseOffset) {
                return current;
            }

            if (offset < current.baseOffset) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }

        } else {
            // Last segment has no upper boundary
            if (offset >= current.baseOffset) {
                return current;
            }

            high = mid - 1;
        }
    }

    return null;
}

//connecting offset to physical position in the .log using .index
private long findPositionForOffset(SegmentInfo segment, long offset) {
    try (RandomAccessFile indexFile =
                 new RandomAccessFile(segment.indexFile, "r")) {

        FileChannel channel = indexFile.getChannel();

        long relativeOffset = offset - segment.baseOffset;
        long indexPosition = relativeOffset * 16;

        if (indexPosition >= channel.size()) {
            return -1;
        }

        channel.position(indexPosition);

        ByteBuffer buffer = ByteBuffer.allocate(16);

        if (channel.read(buffer) < 16) {
            return 0;
        }

        buffer.flip();

        buffer.getLong(); // indexed offset
        return buffer.getLong(); // log position

    } catch (IOException e) {
        throw new RuntimeException(
                "Failed to find message position", e);
    }
}

//find offset position when broker restarts
private long findNextOffset(SegmentInfo segment) {
    try (RandomAccessFile file =
                 new RandomAccessFile(segment.logFile, "r")) {

        FileChannel channel = file.getChannel();

        long offset = segment.baseOffset;

        while (channel.position() < channel.size()) {

            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

            if (channel.read(sizeBuffer) < 4) {
                break;
            }

            sizeBuffer.flip();

            int messageSize = sizeBuffer.getInt();

            channel.position(
                    channel.position() + messageSize
            );

            offset++;
        }

        return offset;

    } catch (IOException e) {
        throw new RuntimeException(
                "Failed to recover offset", e);
    }
}

//fn which creates the first peice of disk storage
private void createNewSegment(long baseOffset) {
    String fileName = String.format("%020d", baseOffset);

    String logFile = baseDir + "/" + fileName + ".log";
    String indexFile = baseDir + "/" + fileName + ".index";

    try {
        File log = new File(logFile);
        File index = new File(indexFile);

        log.createNewFile();
        index.createNewFile();

        SegmentInfo segment =
                new SegmentInfo(baseOffset, logFile, indexFile);

        segments.add(segment);

        openSegmentForAppend(segment);

    } catch (IOException e) {
        throw new RuntimeException(
                "Failed to create new segment", e);
    }
}

//keep writing consistent and not overwrite
private void openSegmentForAppend(SegmentInfo segment) {
    try {
        // Close the previously active segment
        if (activeLogChannel != null) {
            activeLogChannel.close();
        }

        if (activeLogFile != null) {
            activeLogFile.close();
        }

        // Open the new segment
        activeLogFile = new RandomAccessFile(segment.logFile, "rw");
        activeLogChannel = activeLogFile.getChannel();

        // Move to the end for append-only writing
        activeLogChannel.position(activeLogChannel.size());

    } catch (IOException e) {
        throw new RuntimeException(
                "Failed to open log segment", e);
    }
}

private void updateIndex(long offset, long position, SegmentInfo segment) {
    try (RandomAccessFile indexFile =
                 new RandomAccessFile(segment.indexFile, "rw")) {

        FileChannel channel = indexFile.getChannel();

        channel.position(channel.size());

        ByteBuffer buffer = ByteBuffer.allocate(16);

        buffer.putLong(offset);
        buffer.putLong(position);

        buffer.flip();

        channel.write(buffer);
        channel.force(true);

    } catch (IOException e) {
        throw new RuntimeException(
                "Failed to update index", e);
    }
}

//methods introduce for partition rebalancing in SimpleKafkaBroker
public int getId() {
    return id;
}

public int getLeader() {
    return leader;
}

public void setLeader(int leader) {
    this.leader = leader;
}

public List<Integer> getFollowers() {
    return new ArrayList<>(followers);
}

public void setFollowers(List<Integer> followers) {
    this.followers = new ArrayList<>(followers);
}

}