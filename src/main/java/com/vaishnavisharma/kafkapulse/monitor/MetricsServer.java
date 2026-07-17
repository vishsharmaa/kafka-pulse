package com.vaishnavisharma.kafkapulse.monitor;

import com.sun.net.httpserver.HttpServer;
import com.vaishnavisharma.kafkapulse.model.GroupLagReport;
import com.vaishnavisharma.kafkapulse.model.PartitionLag;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tiny zero-dependency HTTP endpoint (JDK built-in HttpServer) exposing the
 * monitor's latest lag reports as JSON at /metrics — the shape a scraper or
 * dashboard would consume.
 */
public final class MetricsServer implements AutoCloseable {

    private final HttpServer server;

    public MetricsServer(int port, LagMonitor monitor) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = toJson(monitor.latestReports()).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.createContext("/health", exchange -> {
            byte[] body = "{\"status\":\"up\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
    }

    static String toJson(Map<String, GroupLagReport> reports) {
        return reports.values().stream()
                .map(MetricsServer::groupJson)
                .collect(Collectors.joining(",", "{\"groups\":[", "]}"));
    }

    private static String groupJson(GroupLagReport report) {
        String partitions = report.partitions().stream()
                .map(MetricsServer::partitionJson)
                .collect(Collectors.joining(","));
        return String.format(
                "{\"groupId\":\"%s\",\"health\":\"%s\",\"totalLag\":%d,\"maxPartitionLag\":%d,"
                        + "\"timestamp\":\"%s\",\"partitions\":[%s]}",
                report.groupId(), report.health(), report.totalLag(),
                report.maxPartitionLag(), report.timestamp(), partitions);
    }

    private static String partitionJson(PartitionLag pl) {
        return String.format(
                "{\"topic\":\"%s\",\"partition\":%d,\"committed\":%d,\"end\":%d,\"lag\":%d}",
                pl.topic(), pl.partition(), pl.committedOffset(), pl.endOffset(), pl.lag());
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
