package com.phantan.mock;

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
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MockPrimaryServer - Node Chủ (Primary / Coordinator) giả lập chuẩn phân tán
 * Dành riêng cho Bạn 2 (Hảo) để kiểm thử độc lập và demo cơ chế Catch-Up Recovery
 * mà không cần phụ thuộc vào tiến độ của Bạn 1.
 *
 * Tính năng chuẩn giáo trình Tanenbaum:
 * 1. Monotonic Version Counter: Tự động tăng v1 -> v2 -> v3... cho mỗi lệnh ghi PUT/DELETE.
 * 2. Write-Ahead Log (WAL): Lưu trữ toàn bộ lịch sử các bản ghi theo thứ tự thời gian.
 * 3. Phục vụ Catch-Up Service (API /api/catchup): Trích xuất danh sách Delta updates
 *    từ v_local + 1 đến v_latest gửi cho Backup bị lỡ nhịp.
 * 4. Chế độ Replication: Hỗ trợ đẩy sang Backup Replicas (cổng 8002, 8003).
 */
public class MockPrimaryServer {
    private final int port;
    private final AtomicLong versionCounter;
    private final Map<String, KeyValueRecord> storage;
    private final List<KeyValueRecord> walLog;
    private final List<String> replicaUrls;
    private final Gson gson;
    private HttpServer server;
    private final long startTimeMillis;

    public MockPrimaryServer(int port) {
        this.port = port;
        this.versionCounter = new AtomicLong(0);
        this.storage = new ConcurrentHashMap<>();
        this.walLog = new CopyOnWriteArrayList<>();
        this.replicaUrls = new CopyOnWriteArrayList<>(Arrays.asList(
                "http://localhost:8002",
                "http://localhost:8003"
        ));
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.startTimeMillis = System.currentTimeMillis();
    }

    public void setReplicaUrls(List<String> urls) {
        this.replicaUrls.clear();
        if (urls != null) {
            this.replicaUrls.addAll(urls);
        }
    }

    public void addReplicaUrl(String url) {
        if (!this.replicaUrls.contains(url)) {
            this.replicaUrls.add(url);
        }
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newCachedThreadPool());

        // Đăng ký API
        server.createContext("/api/put", new PutHandler());
        server.createContext("/api/delete", new DeleteHandler());
        server.createContext("/api/catchup", new CatchUpServiceHandler());
        server.createContext("/api/status", new StatusHandler());
        server.createContext("/api/wal", new WalDumpHandler());
        server.createContext("/api/clear", new ClearHandler());

        server.start();

        System.out.println("======================================================================");
        System.out.printf("  [MOCK PRIMARY NODE] DANG CHAY TAI PORT %d%n", port);
        System.out.printf("  - Version hien tai : v%d%n", versionCounter.get());
        System.out.printf("  - Replicas muc tieu: %s%n", replicaUrls);
        System.out.println("  - Endpoint phuc vu Catch-Up cho Ban 2: POST /api/catchup");
        System.out.println("  - Endpoint nap du lieu ghi:            POST /api/put");
        System.out.println("======================================================================");
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            System.out.println("[PRIMARY] Da dung server.");
        }
    }

    /**
     * Ghi một bản ghi vào Primary (tăng version đơn điệu, append WAL, và replicate sang Backup)
     */
    public synchronized KeyValueRecord write(String key, String value, boolean replicate) {
        long newVersion = versionCounter.incrementAndGet();
        KeyValueRecord record = new KeyValueRecord(key, value, newVersion, System.currentTimeMillis(), false);
        storage.put(key, record);
        walLog.add(record);

        System.out.printf("[PRIMARY-WRITE] PUT key='%s', value='%s' -> Cap version moi: v%d%n",
                key, value, newVersion);

        if (replicate) {
            replicateToBackups(record);
        }

        return record;
    }

    private void replicateToBackups(KeyValueRecord record) {
        String json = gson.toJson(record);
        for (String replicaUrl : replicaUrls) {
            CompletableFuture.runAsync(() -> {
                try {
                    URL url = URI.create(replicaUrl + "/api/replicate").toURL();
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("POST");
                    conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                    conn.setConnectTimeout(1000);
                    conn.setReadTimeout(1000);
                    conn.setDoOutput(true);
                    try (OutputStream os = conn.getOutputStream()) {
                        os.write(json.getBytes(StandardCharsets.UTF_8));
                        os.flush();
                    }
                    int code = conn.getResponseCode();
                    if (code == 200) {
                        System.out.printf("[PRIMARY-REPL] Da replicate thanh cong v%d toi %s%n",
                                record.getVersion(), replicaUrl);
                    }
                } catch (Exception e) {
                    // Node backup co the dang offline (sập node) - day chinh la kịch bản cần catch-up!
                    System.out.printf("[PRIMARY-REPL-OFFLINE] %s dang offline (khong the replicate v%d).%n",
                            replicaUrl, record.getVersion());
                }
            });
        }
    }

    // ==========================================
    // HTTP Handlers
    // ==========================================

    /**
     * POST /api/catchup
     * TRỌNG TÂM CỦA BẠN 2: Primary trích xuất WAL gửi bù cho Backup
     */
    private class CatchUpServiceHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\": \"Method Not Allowed\"}");
                return;
            }

            String body = readRequestBody(exchange);
            CatchUpRequest req = gson.fromJson(body, CatchUpRequest.class);

            if (req == null) {
                sendResponse(exchange, 400, "{\"error\": \"Invalid CatchUpRequest body\"}");
                return;
            }

            long fromVersion = req.getFromVersion();
            long currentPrimaryVersion = versionCounter.get();

            System.out.println("----------------------------------------------------------------------");
            System.out.printf("[PRIMARY-CATCHUP-REQ] Nhan yeu cau Catch-Up tu: %s%n", req.getReplicaId());
            System.out.printf("                      - Version hien tai cua Replica: v%d%n", fromVersion);
            System.out.printf("                      - Version hien tai cua Primary: v%d%n", currentPrimaryVersion);

            // Loc toan bo ban ghi trong WAL co version > fromVersion
            List<KeyValueRecord> missingRecords = new ArrayList<>();
            for (KeyValueRecord record : walLog) {
                if (record.getVersion() > fromVersion) {
                    if (req.getTargetVersion() <= 0 || record.getVersion() <= req.getTargetVersion()) {
                        missingRecords.add(record);
                    }
                }
            }

            // Sắp xếp tăng dần theo version để Backup replay đúng thứ tự
            missingRecords.sort(Comparator.comparingLong(KeyValueRecord::getVersion));

            CatchUpResponse resp = new CatchUpResponse(
                    req.getReplicaId(),
                    fromVersion,
                    currentPrimaryVersion,
                    missingRecords
            );

            System.out.printf("[PRIMARY-CATCHUP-RESP] Gui tra %d ban ghi bu (tu v%d den v%d) cho %s%n",
                    missingRecords.size(),
                    (missingRecords.isEmpty() ? fromVersion : missingRecords.get(0).getVersion()),
                    currentPrimaryVersion,
                    req.getReplicaId());
            System.out.println("----------------------------------------------------------------------");

            sendResponse(exchange, 200, gson.toJson(resp));
        }
    }

    /**
     * POST /api/put
     * Client ghi dữ liệu vào Primary
     */
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
                sendResponse(exchange, 400, "{\"error\": \"Can co 'key' va 'value'\"}");
                return;
            }

            String key = payload.get("key");
            String value = payload.get("value");
            boolean replicate = !"false".equalsIgnoreCase(payload.get("replicate"));

            KeyValueRecord record = write(key, value, replicate);

            Map<String, Object> resp = new HashMap<>();
            resp.put("success", true);
            resp.put("key", record.getKey());
            resp.put("value", record.getValue());
            resp.put("version", record.getVersion());
            resp.put("timestamp", record.getTimestamp());

            sendResponse(exchange, 200, gson.toJson(resp));
        }
    }

    /**
     * POST /api/delete
     */
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
                sendResponse(exchange, 400, "{\"error\": \"Can co 'key'\"}");
                return;
            }

            long newVersion = versionCounter.incrementAndGet();
            KeyValueRecord record = new KeyValueRecord(key, null, newVersion, System.currentTimeMillis(), true);
            storage.remove(key);
            walLog.add(record);

            replicateToBackups(record);

            Map<String, Object> resp = new HashMap<>();
            resp.put("success", true);
            resp.put("key", key);
            resp.put("version", newVersion);
            resp.put("isDeleted", true);

            sendResponse(exchange, 200, gson.toJson(resp));
        }
    }

    /**
     * GET /api/status
     */
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
                    versionCounter.get(),
                    storage.size(),
                    0,
                    uptime
            );

            sendResponse(exchange, 200, gson.toJson(status));
        }
    }

    /**
     * GET /api/wal
     */
    private class WalDumpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("currentVersion", versionCounter.get());
            resp.put("walLength", walLog.size());
            resp.put("walEntries", walLog);

            sendResponse(exchange, 200, gson.toJson(resp));
        }
    }

    /**
     * POST /api/clear
     */
    private class ClearHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            storage.clear();
            walLog.clear();
            versionCounter.set(0);
            sendResponse(exchange, 200, "{\"status\": \"OK\", \"message\": \"Da reset Primary ve v0\"}");
        }
    }

    // ==========================================
    // Utilities
    // ==========================================

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

    public static void main(String[] args) {
        int port = 8001;
        if (args.length >= 1) {
            try { port = Integer.parseInt(args[0]); } catch (Exception ignored) {}
        }
        try {
            MockPrimaryServer server = new MockPrimaryServer(port);
            server.start();
            System.out.println("Nhan Ctrl + C de thoat.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
