package com.vaishnavisharma.kafkapulse.monitor;

import com.vaishnavisharma.kafkapulse.model.GroupLagReport;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LagCalculatorTest {

    private final LagCalculator calculator = new LagCalculator(100, 1000);

    @Test
    void computesLagAcrossPartitions() {
        Map<TopicPartition, Long> committed = new HashMap<>();
        committed.put(tp(0), 90L);
        committed.put(tp(1), 10L);

        Map<TopicPartition, Long> end = new HashMap<>();
        end.put(tp(0), 100L);
        end.put(tp(1), 50L);

        GroupLagReport report = calculator.report("g1", committed, end);

        assertEquals(50, report.totalLag());
        assertEquals(40, report.maxPartitionLag());
        assertEquals(GroupLagReport.Health.OK, report.health());
    }

    @Test
    void healthDrivenByWorstPartitionNotTotal() {
        // total lag is small, but one partition alone crosses the warn threshold
        Map<TopicPartition, Long> committed = Map.of(tp(0), 0L, tp(1), 100L);
        Map<TopicPartition, Long> end = Map.of(tp(0), 150L, tp(1), 100L);

        GroupLagReport report = calculator.report("g1", committed, end);

        assertEquals(GroupLagReport.Health.WARNING, report.health());
    }

    @Test
    void crossingCriticalThresholdFlagsCritical() {
        Map<TopicPartition, Long> committed = Map.of(tp(0), 0L);
        Map<TopicPartition, Long> end = Map.of(tp(0), 5000L);

        GroupLagReport report = calculator.report("g1", committed, end);

        assertEquals(GroupLagReport.Health.CRITICAL, report.health());
    }

    @Test
    void partitionMissingFromCommittedOffsetsCountsWholeLog() {
        // group never committed to partition 1
        Map<TopicPartition, Long> committed = Map.of(tp(0), 100L);
        Map<TopicPartition, Long> end = Map.of(tp(0), 100L, tp(1), 80L);

        GroupLagReport report = calculator.report("g1", committed, end);

        assertEquals(80, report.totalLag());
        assertEquals(2, report.partitions().size());
    }

    @Test
    void emptyMapsProduceHealthyEmptyReport() {
        GroupLagReport report = calculator.report("g1", Map.of(), Map.of());

        assertEquals(0, report.totalLag());
        assertEquals(GroupLagReport.Health.OK, report.health());
    }

    private static TopicPartition tp(int partition) {
        return new TopicPartition("orders", partition);
    }
}
