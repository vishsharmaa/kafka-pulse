package com.vaishnavisharma.kafkapulse.monitor;

import com.vaishnavisharma.kafkapulse.model.GroupLagReport;
import com.vaishnavisharma.kafkapulse.model.PartitionLag;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Pure lag-computation logic, deliberately separated from the Kafka AdminClient
 * plumbing in {@link LagMonitor} so it can be unit-tested without a broker.
 */
public final class LagCalculator {

    private final long warnThreshold;
    private final long criticalThreshold;

    public LagCalculator(long warnThreshold, long criticalThreshold) {
        this.warnThreshold = warnThreshold;
        this.criticalThreshold = criticalThreshold;
    }

    /**
     * Joins a consumer group's committed offsets against the current log-end offsets.
     *
     * Every partition present in {@code endOffsets} is reported. A partition the
     * group has never committed to is reported with committedOffset = -1, which
     * {@link PartitionLag} treats as "entire log unread".
     */
    public GroupLagReport report(String groupId,
                                 Map<TopicPartition, Long> committedOffsets,
                                 Map<TopicPartition, Long> endOffsets) {
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(committedOffsets, "committedOffsets");
        Objects.requireNonNull(endOffsets, "endOffsets");

        List<PartitionLag> lags = new ArrayList<>(endOffsets.size());
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long end = entry.getValue();
            long committed = committedOffsets.getOrDefault(tp, -1L);
            lags.add(new PartitionLag(tp.topic(), tp.partition(), committed, end));
        }
        lags.sort(Comparator.comparing(PartitionLag::topic)
                .thenComparingInt(PartitionLag::partition));

        return new GroupLagReport(groupId, Instant.now(), lags, warnThreshold, criticalThreshold);
    }
}
