package com.phantan.primary;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.phantan.common.CatchUpRequest;
import com.phantan.common.CatchUpResponse;
import com.phantan.common.KeyValueRecord;
import com.phantan.common.NodeStatus;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * [MODULE BẠN 1 PHỤ TRÁCH]
 * PrimaryServer - Node Chủ (Coordinator / Leader) chạy tại cổng 8001.
 *
 * Nhiệm vụ theo đề bài:
 * 1. Tiếp nhận mọi yêu cầu ghi PUT(key, value) và xóa DELETE(key) từ Client.
 * 2. Gán số Version tăng đơn điệu (Total Ordering).
 * 3. Điều phối nhân bản (Sync vs Async) sang 2 Backup Replicas.
 * 4. Duy trì Write-Ahead Log (WAL) và phục vụ API /api/catchup cho Bạn 2.
 */
public class PrimaryServer {
    private final int port;
    private final VersionCounter versionCounter;
    private final WriteCoordinator coordinator;
    private final Map<String, KeyValueRecord> storage;
    private final List<KeyValueRecord> walLog;
    private final Gson gson;
    private HttpServer server;
    private final long startTimeMillis;

    public PrimaryServer(int port, List<String> replicaUrls) {
        this.port = port;
        this.versionCounter = new VersionCounter();
        this.coordinator = new WriteCoordinator(replicaUrls);
        this.storage = new ConcurrentHashMap<>();
        this.walLog = new CopyOnWriteArrayList<>();
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.startTimeMillis = System.currentTimeMillis();
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newCachedThreadPool());

        server.createContext("/api/put", new PutHandler());
        server.createContext("/api/delete", new DeleteHandler());
        server.createContext("/api/catchup", new CatchUpServiceHandler());
        server.createContext("/api/status", new StatusHandler());
        server.createContext("/api/wal", new WalDumpHandler());
        server.createContext("/api/mode", new ReplicationModeHandler());
        server.createContext("/api/clear", new ClearHandler());

        server.start();

        System.out.println("======================================================================");
        System.out.printf("  [PRIMARY NODE] DANG CHAY TAI PORT %d%n", port);
        System.out.printf("  - Che do Replication mac dinh: %s%n", coordinator.getCurrentMode());
        System.out.println("======================================================================");
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private class PutHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            String body = readRequestBody(exchange);
            Map<String, String> payload = gson.fromJson(body, Map.class);
            if (payload == null || !payload.containsKey("key") || !payload.containsKey("value")) {
                sendResponse(exchange, 400, "{\"error\": \"Yeu cau co 'key' va 'value'\"}");
                return;
            }

            String key = payload.get("key");
            String value = payload.get("value");

            long newVersion = versionCounter.nextVersion();
            KeyValueRecord record = new KeyValueRecord(key, value, newVersion, System.currentTimeMillis(), false);

            storage.put(key, record);
            walLog.add(record);

            // Nhân bản sang các Backup Replicas theo Sync/Async
            coordinator.replicate(record);

            Map<String, Object> resp = new HashMap<>();
            resp.put("success", true);
            resp.put("key", record.getKey());
            resp.put("value", record.getValue());
            resp.put("version", record.getVersion());
            resp.put("timestamp", record.getTimestamp());
            resp.put("replicationMode", coordinator.getCurrentMode().name());

            sendResponse(exchange, 200, gson.toJson(resp));
        }
    }

    private class DeleteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            String body = readRequestBody(exchange);
            Map<String, String> payload = gson.fromJson(body, Map.class);
            String key = (payload != null) ? payload.get("key") : null;

            if (key == null) {
                sendResponse(exchange, 400, "{\"error\": \"Yeu cau co 'key'\"}");
                return;
            }

            long newVersion = versionCounter.nextVersion();
            KeyValueRecord record = new KeyValueRecord(key, null, newVersion, System.currentTimeMillis(), true);

            storage.remove(key);
            walLog.add(record);

            coordinator.replicate(record);

            Map<String, Object> resp = new HashMap<>();
            resp.put("success", true);
            resp.put("key", key);
            resp.put("version", newVersion);
            resp.put("isDeleted", true);

            sendResponse(exchange, 200, gson.toJson(resp));
        }
    }

    private class CatchUpServiceHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            String body = readRequestBody(exchange);
            CatchUpRequest req = gson.fromJson(body, CatchUpRequest.class);

            if (req == null) {
                sendResponse(exchange, 400, "{\"error\": \"Body CatchUpRequest khong hop le\"}");
                return;
            }

            long fromVersion = req.getFromVersion();
            long currentPrimaryVersion = versionCounter.getCurrentVersion();

            List<KeyValueRecord> missingRecords = new ArrayList<>();
            for (KeyValueRecord record : walLog) {
                if (record.getVersion() > fromVersion) {
                    if (req.getTargetVersion() <= 0 || record.getVersion() <= req.getTargetVersion()) {
                        missingRecords.add(record);
                    }
                }
            }
            missingRecords.sort(Comparator.comparingLong(KeyValueRecord::getVersion));

            CatchUpResponse resp = new CatchUpResponse(req.getReplicaId(), fromVersion, currentPrimaryVersion, missingRecords);
            sendResponse(exchange, 200, gson.toJson(resp));
        }
    }

    private class ReplicationModeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            Map<String, String> query = parseQuery(exchange.getRequestURI().getQuery());
            String modeStr = query.get("mode");
            if (modeStr != null) {
                if ("ASYNC".equalsIgnoreCase(modeStr)) {
                    coordinator.setCurrentMode(WriteCoordinator.Mode.ASYNC);
                } else {
                    coordinator.setCurrentMode(WriteCoordinator.Mode.SYNC);
                }
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("mode", coordinator.getCurrentMode().name());
            sendResponse(exchange, 200, gson.toJson(resp));
        }
    }

    private class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            long uptime = (System.currentTimeMillis() - startTimeMillis) / 1000;
            NodeStatus status = new NodeStatus(
                    "primary-1",
                    "PRIMARY",
                    port,
                    "ONLINE",
                    versionCounter.getCurrentVersion(),
                    storage.size(),
                    0,
                    uptime
            );
            sendResponse(exchange, 200, gson.toJson(status));
        }
    }

    private class WalDumpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("currentVersion", versionCounter.getCurrentVersion());
            resp.put("walLength", walLog.size());
            resp.put("walEntries", walLog);
            sendResponse(exchange, 200, gson.toJson(resp));
        }
    }

    private class ClearHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            storage.clear();
            walLog.clear();
            versionCounter.reset();
            sendResponse(exchange, 200, "{\"status\": \"OK\", \"message\": \"Da reset Primary ve v0\"}");
        }
    }

    private void addCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, Authorization");
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String responseBody) throws IOException {
        byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
            os.flush();
        }
    }

    private String readRequestBody(HttpExchange exchange) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> result = new HashMap<>();
        if (query == null || query.isEmpty()) return result;
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            try {
                if (idx > 0) {
                    result.put(URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8.name()),
                            URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8.name()));
                }
            } catch (Exception ignored) {}
        }
        return result;
    }

    public static void main(String[] args) {
        int port = 8001;
        List<String> replicas = Arrays.asList("http://localhost:8002", "http://localhost:8003");
        try {
            PrimaryServer server = new PrimaryServer(port, replicas);
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
