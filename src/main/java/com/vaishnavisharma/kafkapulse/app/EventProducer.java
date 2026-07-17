package com.vaishnavisharma.kafkapulse.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * Simulates a stream of order events so the monitor has real traffic to measure.
 * Keys are user IDs, so events for the same user always land on the same partition
 * (per Kafka's default key-hash partitioner) and stay ordered.
 */
public final class EventProducer {

    public static final String TOPIC = "orders";

    private EventProducer() {
    }

    public static void run(String bootstrapServers, int eventsPerSecond) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks=all + idempotence: every event is written exactly once to the leader
        // and its replicas, at the cost of a little latency. Right trade-off for orders.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        Random random = new Random();
        long sleepMillis = Math.max(1, 1000L / Math.max(1, eventsPerSecond));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.printf("Producing ~%d events/sec to topic '%s'. Ctrl+C to stop.%n",
                    eventsPerSecond, TOPIC);
            long sent = 0;
            while (!Thread.currentThread().isInterrupted()) {
                String userId = "user-" + random.nextInt(50);
                String payload = String.format(
                        "{\"userId\":\"%s\",\"amount\":%d,\"ts\":%d}",
                        userId, 100 + random.nextInt(900), System.currentTimeMillis());

                producer.send(new ProducerRecord<>(TOPIC, userId, payload), (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Send failed: " + exception.getMessage());
                    }
                });

                if (++sent % 500 == 0) {
                    System.out.println("sent=" + sent);
                }
                Thread.sleep(sleepMillis);
            }
        }
    }
}
