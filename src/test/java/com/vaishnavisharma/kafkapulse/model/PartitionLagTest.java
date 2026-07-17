package com.vaishnavisharma.kafkapulse.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartitionLagTest {

    @Test
    void lagIsEndMinusCommitted() {
        PartitionLag lag = new PartitionLag("orders", 0, 40, 100);
        assertEquals(60, lag.lag());
    }

    @Test
    void caughtUpConsumerHasZeroLag() {
        PartitionLag lag = new PartitionLag("orders", 1, 100, 100);
        assertEquals(0, lag.lag());
    }

    @Test
    void committedAheadOfStaleEndOffsetClampsToZero() {
        // End offsets and committed offsets are fetched in separate calls, so the
        // committed value can be momentarily newer than the end snapshot.
        PartitionLag lag = new PartitionLag("orders", 2, 105, 100);
        assertEquals(0, lag.lag());
    }

    @Test
    void neverCommittedPartitionReportsWholeLogAsLag() {
        PartitionLag lag = new PartitionLag("orders", 3, -1, 250);
        assertEquals(250, lag.lag());
    }

    @Test
    void emptyPartitionWithNoCommitsHasZeroLag() {
        PartitionLag lag = new PartitionLag("orders", 4, -1, 0);
        assertEquals(0, lag.lag());
    }

    @Test
    void negativeEndOffsetIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new PartitionLag("orders", 0, 0, -5));
    }
}
