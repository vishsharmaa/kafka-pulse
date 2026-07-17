package com.vaishnavisharma.kafkapulse.monitor;

import com.vaishnavisharma.kafkapulse.model.GroupLagReport;
import com.vaishnavisharma.kafkapulse.model.PartitionLag;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Polls the cluster with the Kafka AdminClient and produces a
 * {@link GroupLagReport} per consumer group on a fixed interval.
 *
 * This is the same core signal Confluent Control Center exposes as
 * "consumer lag": committed group offsets joined against log-end offsets.
 */
public final class LagMonitor implements AutoCloseable {

    private final Admin admin;
    private final LagCalculator calculator;
    private final Map<String, GroupLagReport> latestReports = new ConcurrentHashMap<>();

    public LagMonitor(String bootstrapServers, long warnThreshold, long criticalThreshold) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        this.admin = Admin.create(props);
        this.calculator = new LagCalculator(warnThreshold, criticalThreshold);
    }

    /** Snapshot of the most recent report per consumer group (read by the metrics server). */
    public Map<String, GroupLagReport> latestReports() {
        return Map.copyOf(latestReports);
    }

    /** Runs one full poll cycle: list groups, fetch offsets, compute lag. */
    public void pollOnce() throws ExecutionException, InterruptedException {
        Collection<ConsumerGroupListing> groups = admin.listConsumerGroups().all().get();
        for (ConsumerGroupListing group : groups) {
            String groupId = group.groupId();
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata().get();
            if (committed.isEmpty()) {
                continue;
            }

            Map<TopicPartition, Long> committedOffsets = committed.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

            Map<TopicPartition, OffsetSpec> latestSpec = committed.keySet().stream()
                    .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                    admin.listOffsets(latestSpec).all().get();

            Map<TopicPartition, Long> endOffsets = new HashMap<>();
            ends.forEach((tp, info) -> endOffsets.put(tp, info.offset()));

            latestReports.put(groupId, calculator.report(groupId, committedOffsets, endOffsets));
        }
    }

    /** Polls forever, printing a console dashboard each cycle. */
    public void run(Duration interval) throws Exception {
        while (!Thread.currentThread().isInterrupted()) {
            pollOnce();
            printDashboard();
            Thread.sleep(interval.toMillis());
        }
    }

    private void printDashboard() {
        System.out.println("\n=== KafkaPulse @ " + java.time.LocalTime.now().withNano(0) + " ===");
        if (latestReports.isEmpty()) {
            System.out.println("(no consumer groups with committed offsets yet)");
            return;
        }
        latestReports.values().forEach(report -> {
            System.out.printf("group=%s health=%s totalLag=%d maxPartitionLag=%d%n",
                    report.groupId(), report.health(), report.totalLag(), report.maxPartitionLag());
            for (PartitionLag pl : report.partitions()) {
                System.out.println("  " + pl);
            }
        });
    }

    @Override
    public void close() {
        admin.close();
    }
}
