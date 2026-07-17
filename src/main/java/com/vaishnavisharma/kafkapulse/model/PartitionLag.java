package com.vaishnavisharma.kafkapulse.model;

import java.util.Objects;

/**
 * Immutable snapshot of a single partition's consumer lag:
 * the distance between the last committed offset of a consumer group
 * and the log-end offset of the partition.
 */
public final class PartitionLag {

    private final String topic;
    private final int partition;
    private final long committedOffset;
    private final long endOffset;

    public PartitionLag(String topic, int partition, long committedOffset, long endOffset) {
        if (endOffset < 0) {
            throw new IllegalArgumentException("endOffset must be >= 0, got " + endOffset);
        }
        this.topic = Objects.requireNonNull(topic, "topic");
        this.partition = partition;
        this.committedOffset = committedOffset;
        this.endOffset = endOffset;
    }

    /** Lag is never negative: a committed offset ahead of a stale end offset reads as 0. */
    public long lag() {
        if (committedOffset < 0) {
            // No committed offset yet: the group has never committed for this partition,
            // so everything in the log is "unread".
            return endOffset;
        }
        return Math.max(0, endOffset - committedOffset);
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long committedOffset() {
        return committedOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        return String.format("%s-%d committed=%d end=%d lag=%d",
                topic, partition, committedOffset, endOffset, lag());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionLag)) return false;
        PartitionLag that = (PartitionLag) o;
        return partition == that.partition
                && committedOffset == that.committedOffset
                && endOffset == that.endOffset
                && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, committedOffset, endOffset);
    }
}
