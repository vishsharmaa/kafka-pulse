package com.vaishnavisharma.kafkapulse.model;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Aggregated lag report for one consumer group at a point in time,
 * with a simple traffic-light health status derived from configurable thresholds.
 */
public final class GroupLagReport {

    public enum Health { OK, WARNING, CRITICAL }

    private final String groupId;
    private final Instant timestamp;
    private final List<PartitionLag> partitions;
    private final long warnThreshold;
    private final long criticalThreshold;

    public GroupLagReport(String groupId,
                          Instant timestamp,
                          List<PartitionLag> partitions,
                          long warnThreshold,
                          long criticalThreshold) {
        if (warnThreshold < 0 || criticalThreshold < warnThreshold) {
            throw new IllegalArgumentException(
                    "Thresholds must satisfy 0 <= warn <= critical, got warn="
                            + warnThreshold + " critical=" + criticalThreshold);
        }
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
        this.partitions = Collections.unmodifiableList(Objects.requireNonNull(partitions, "partitions"));
        this.warnThreshold = warnThreshold;
        this.criticalThreshold = criticalThreshold;
    }

    public long totalLag() {
        return partitions.stream().mapToLong(PartitionLag::lag).sum();
    }

    public long maxPartitionLag() {
        return partitions.stream().mapToLong(PartitionLag::lag).max().orElse(0L);
    }

    /**
     * Health is driven by the worst single partition, not the total:
     * one stuck partition is an incident even if the rest of the group is keeping up.
     */
    public Health health() {
        long worst = maxPartitionLag();
        if (worst >= criticalThreshold) return Health.CRITICAL;
        if (worst >= warnThreshold) return Health.WARNING;
        return Health.OK;
    }

    public String groupId() {
        return groupId;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public List<PartitionLag> partitions() {
        return partitions;
    }
}
