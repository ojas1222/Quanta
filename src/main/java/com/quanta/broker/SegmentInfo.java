//Description/metadata of log segment

package com.quanta.broker;

public class SegmentInfo {

    public final long baseOffset;
    public final String logFile;
    public final String indexFile;

    public SegmentInfo(long baseOffset, String logFile, String indexFile) {
        this.baseOffset = baseOffset;
        this.logFile = logFile;
        this.indexFile = indexFile;
    }
}