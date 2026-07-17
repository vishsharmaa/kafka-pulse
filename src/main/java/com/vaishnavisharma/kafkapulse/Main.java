package com.vaishnavisharma.kafkapulse;

import com.vaishnavisharma.kafkapulse.app.EventConsumer;
import com.vaishnavisharma.kafkapulse.app.EventProducer;
import com.vaishnavisharma.kafkapulse.monitor.LagMonitor;
import com.vaishnavisharma.kafkapulse.monitor.MetricsServer;

import java.time.Duration;

/**
 * Single entry point; the first argument picks the role:
 *
 *   produce  — simulate order events onto the 'orders' topic
 *   consume  — process them as consumer group 'orders-processor'
 *   monitor  — poll consumer lag + serve JSON metrics on :8080
 *
 * Config via env vars: BOOTSTRAP_SERVERS (default localhost:9092),
 * EVENTS_PER_SEC, PROCESS_DELAY_MS, WARN_LAG, CRITICAL_LAG, METRICS_PORT.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        String bootstrap = env("BOOTSTRAP_SERVERS", "localhost:9092");
        String mode = args.length > 0 ? args[0] : "monitor";

        switch (mode) {
            case "produce":
                EventProducer.run(bootstrap, intEnv("EVENTS_PER_SEC", 50));
                break;
            case "consume":
                EventConsumer.run(bootstrap, intEnv("PROCESS_DELAY_MS", 0));
                break;
            case "monitor":
                long warn = intEnv("WARN_LAG", 100);
                long critical = intEnv("CRITICAL_LAG", 1000);
                try (LagMonitor monitor = new LagMonitor(bootstrap, warn, critical);
                     MetricsServer ignored = new MetricsServer(intEnv("METRICS_PORT", 8080), monitor)) {
                    System.out.println("Metrics at http://localhost:" + intEnv("METRICS_PORT", 8080) + "/metrics");
                    monitor.run(Duration.ofSeconds(5));
                }
                break;
            default:
                System.err.println("Usage: java -jar kafka-pulse.jar [produce|consume|monitor]");
                System.exit(2);
        }
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }

    private static int intEnv(String key, int fallback) {
        try {
            return Integer.parseInt(env(key, String.valueOf(fallback)));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
