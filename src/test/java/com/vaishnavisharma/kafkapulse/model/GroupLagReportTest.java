package com.vaishnavisharma.kafkapulse.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroupLagReportTest {

    private static final Instant NOW = Instant.parse("2026-07-17T10:00:00Z");

    @Test
    void totalLagSumsAllPartitions() {
        List<PartitionLag> partitions = List.of(
                new PartitionLag("orders", 0, 10, 50),
                new PartitionLag("orders", 1, 20, 80),
                new PartitionLag("orders", 2, 30, 30));

        GroupLagReport report = new GroupLagReport("g1", NOW, partitions, 100, 1000);

        // (50-10) + (80-20) + (30-30) = 40 + 60 + 0 = 100
        assertEquals(100, report.totalLag());
    }

    @Test
    void maxPartitionLagReturnsWorstSinglePartition() {
        List<PartitionLag> partitions = List.of(
                new PartitionLag("orders", 0, 10, 50),  // lag=40
                new PartitionLag("orders", 1, 0, 200)); // lag=200

        GroupLagReport report = new GroupLagReport("g1", NOW, partitions, 100, 1000);

        assertEquals(200, report.maxPartitionLag());
    }

    @Test
    void healthOkWhenAllPartitionsBelowWarnThreshold() {
        List<PartitionLag> partitions = List.of(
                new PartitionLag("orders", 0, 50, 99)); // lag=49

        GroupLagReport report = new GroupLagReport("g1", NOW, partitions, 50, 1000);

        assertEquals(GroupLagReport.Health.OK, report.health());
    }

    @Test
    void healthWarningWhenWorstPartitionMeetsWarnThreshold() {
        List<PartitionLag> partitions = List.of(
                new PartitionLag("orders", 0, 0, 100)); // lag=100 == warn

        GroupLagReport report = new GroupLagReport("g1", NOW, partitions, 100, 1000);

        assertEquals(GroupLagReport.Health.WARNING, report.health());
    }

    @Test
    void healthCriticalWhenWorstPartitionMeetsCriticalThreshold() {
        List<PartitionLag> partitions = List.of(
                new PartitionLag("orders", 0, 0, 1000)); // lag=1000 == critical

        GroupLagReport report = new GroupLagReport("g1", NOW, partitions, 100, 1000);

        assertEquals(GroupLagReport.Health.CRITICAL, report.health());
    }

    @Test
    void emptyPartitionListIsHealthyWithZeroLag() {
        GroupLagReport report = new GroupLagReport("g1", NOW, Collections.emptyList(), 100, 1000);

        assertEquals(0, report.totalLag());
        assertEquals(0, report.maxPartitionLag());
        assertEquals(GroupLagReport.Health.OK, report.health());
    }

    @Test
    void partitionsListIsUnmodifiable() {
        List<PartitionLag> partitions = List.of(
                new PartitionLag("orders", 0, 0, 50));

        GroupLagReport report = new GroupLagReport("g1", NOW, partitions, 100, 1000);

        assertThrows(UnsupportedOperationException.class,
                () -> report.partitions().add(new PartitionLag("orders", 1, 0, 100)));
    }

    @Test
    void rejectsNegativeWarnThreshold() {
        assertThrows(IllegalArgumentException.class,
                () -> new GroupLagReport("g1", NOW, Collections.emptyList(), -1, 1000));
    }

    @Test
    void rejectsWarnGreaterThanCritical() {
        assertThrows(IllegalArgumentException.class,
                () -> new GroupLagReport("g1", NOW, Collections.emptyList(), 500, 100));
    }

    @Test
    void toStringContainsKeyFields() {
        List<PartitionLag> partitions = List.of(
                new PartitionLag("orders", 0, 10, 50));

        GroupLagReport report = new GroupLagReport("g1", NOW, partitions, 100, 1000);
        String str = report.toString();

        assertTrue(str.contains("g1"));
        assertTrue(str.contains("OK"));
        assertTrue(str.contains("40"));   // totalLag and maxPartitionLag
    }

    @Test
    void accessorsReturnConstructorValues() {
        List<PartitionLag> partitions = List.of(
                new PartitionLag("orders", 0, 0, 50));

        GroupLagReport report = new GroupLagReport("my-group", NOW, partitions, 100, 1000);

        assertEquals("my-group", report.groupId());
        assertEquals(NOW, report.timestamp());
        assertEquals(1, report.partitions().size());
    }
}
