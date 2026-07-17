package com.vaishnavisharma.kafkapulse.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Consumer-group worker for the 'orders' topic.
 *
 * Uses manual offset commits *after* processing, so a crash mid-batch means
 * at-least-once redelivery rather than silent loss. An optional artificial
 * delay per record lets you make the group fall behind on purpose and watch
 * the monitor flip from OK to WARNING to CRITICAL.
 */
public final class EventConsumer {

    public static final String GROUP_ID = "orders-processor";

    private EventConsumer() {
    }

    public static void run(String bootstrapServers, long processingDelayMillis) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(EventProducer.TOPIC));
            System.out.printf("Consuming '%s' as group '%s' (delay=%dms/record). Ctrl+C to stop.%n",
                    EventProducer.TOPIC, GROUP_ID, processingDelayMillis);

            long processed = 0;
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    process(record, processingDelayMillis);
                    processed++;
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    if (processed % 500 < records.count()) {
                        System.out.println("processed=" + processed);
                    }
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException ignored) {
            // normal shutdown path
        }
    }

    private static void process(ConsumerRecord<String, String> record, long delayMillis) {
        // Stand-in for real work (DB write, downstream call, etc.)
        if (delayMillis > 0) {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
