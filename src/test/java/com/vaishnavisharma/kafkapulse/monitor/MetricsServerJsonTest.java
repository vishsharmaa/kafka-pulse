package com.vaishnavisharma.kafkapulse.monitor;

import com.vaishnavisharma.kafkapulse.model.GroupLagReport;
import com.vaishnavisharma.kafkapulse.model.PartitionLag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsServerJsonTest {

    @Test
    void emptyReportsSerializeToEmptyGroupArray() {
        assertEquals("{\"groups\":[]}", MetricsServer.toJson(Map.of()));
    }

    @Test
    void reportSerializesGroupHealthAndPartitions() {
        GroupLagReport report = new GroupLagReport(
                "orders-processor",
                Instant.parse("2026-07-17T10:00:00Z"),
                List.of(new PartitionLag("orders", 0, 40, 100)),
                100, 1000);

        String json = MetricsServer.toJson(Map.of("orders-processor", report));

        assertTrue(json.contains("\"groupId\":\"orders-processor\""));
        assertTrue(json.contains("\"totalLag\":60"));
        assertTrue(json.contains("\"health\":\"OK\""));
        assertTrue(json.contains("\"partition\":0"));
        assertTrue(json.contains("\"lag\":60"));
    }
}
