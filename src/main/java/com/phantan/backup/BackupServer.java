package com.phantan.backup;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BackupServer - Khởi chạy một tiến trình Backup Replica Node độc lập.
 *
 * Đóng vai trò:
 * 1. Lưu trữ bản sao bền vững trên đĩa cứng (Fault-tolerant persistence).
 * 2. Nhận các bản ghi từ luồng Replication của Primary (Sync hoặc Async).
 * 3. Phục vụ truy vấn đọc GET(key) kèm version cho Client.
 * 4. Tự động phục hồi trạng thái (Catch-Up Recovery) khi khởi động lại.
 * 5. Giả lập độ trễ mạng (Network Delay Simulation) để hỗ trợ kịch bản demo Read-Your-Writes của Bạn 3.
 */
public class BackupServer {
    private final String replicaId;
    private final int port;
    private final String primaryUrl;
    private final LocalStorage localStorage;
    private final CatchUpEngine catchUpEngine;
    private final Gson gson;
    private final long startTimeMillis;

    private volatile String state = "ONLINE"; // ONLINE, CATCHING_UP, SYNCHRONIZED, SIMULATING_LAG
    private final AtomicLong simulatedDelayMs = new AtomicLong(0);
    private HttpServer server;

    public BackupServer(String replicaId, int port, String dataDir, String primaryUrl) {
        this.replicaId = replicaId;
        this.port = port;
        this.primaryUrl = primaryUrl;
        this.localStorage = new LocalStorage(dataDir);
        this.catchUpEngine = new CatchUpEngine(replicaId, localStorage, primaryUrl);
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.startTimeMillis = System.currentTimeMillis();

        this.catchUpEngine.setStateChangeListener(newState -> {
            this.state = newState;
        });
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newCachedThreadPool());

        // Đăng ký các Endpoint RESTful
        server.createContext("/api/get", new GetHandler());
        server.createContext("/api/replicate", new ReplicateHandler());
        server.createContext("/api/status", new StatusHandler());
        server.createContext("/api/data", new DataDumpHandler());
        server.createContext("/api/catchup", new TriggerCatchUpHandler());
        server.createContext("/api/admin/delay", new SetDelayHandler());
        server.createContext("/api/admin/clear", new ClearDataHandler());
        server.createContext("/api/admin/crash", new SimulateCrashHandler());

        server.start();

        printBanner();

        // Tự động kích hoạt Catch-Up ngay khi khởi động lại để bù đắp dữ liệu bị lỡ (Catch-Up on Startup)
        new Thread(() -> {
            try {
                Thread.sleep(1000); // Cho server ổn định
                System.out.println("[STARTUP-RECOVERY] Tu dong goi Primary de kiem tra catch-up...");
                catchUpEngine.performCatchUp();
            } catch (Exception e) {
                System.err.println("[STARTUP-RECOVERY] Primary chua san sang: " + e.getMessage());
            }
        }, "AutoCatchUpThread-" + replicaId).start();
    }

    public void stop() {
        if (server != null) {
            catchUpEngine.stop();
            server.stop(0);
            System.out.printf("[BACKUP-%s] Server tai cong %d da dung.%n", replicaId, port);
        }
    }

    private void printBanner() {
        System.out.println("======================================================================");
        System.out.printf("  [BACKUP NODE] %s DANG CHAY TAI PORT %d%n", replicaId.toUpperCase(), port);
        System.out.printf("  - Thu muc luu tru dia: %s%n", localStorage.getDataDir());
        System.out.printf("  - Primary Node URL    : %s%n", primaryUrl);
        System.out.printf("  - Version hien co     : v%d%n", localStorage.getCurrentVersion());
        System.out.printf("  - Tong so keys hien co: %d%n", localStorage.getActiveKeysCount());
        System.out.println("  - Cac API ho tro:");
        System.out.printf("      GET  http://localhost:%d/api/get?key=...\n", port);
        System.out.printf("      POST http://localhost:%d/api/replicate\n", port);
        System.out.printf("      GET  http://localhost:%d/api/status\n", port);
        System.out.printf("      GET  http://localhost:%d/api/data\n", port);
        System.out.printf("      POST http://localhost:%d/api/catchup\n", port);
        System.out.printf("      POST http://localhost:%d/api/admin/delay?ms=3000\n", port);
        System.out.println("======================================================================");
    }

    // ==========================================
    // HTTP Handlers
    // ==========================================

    /**
     * GET /api/get?key=...
     * Phục vụ lệnh đọc kèm version cho Client. Hỗ trợ mô phỏng độ trễ nếu có.
     */
    private class GetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            // Giả lập độ trễ mạng nếu đang bật chế độ Lag (phục vụ demo Read-Your-Writes)
            long delay = simulatedDelayMs.get();
            if (delay > 0) {
                try {
                    System.out.printf("[%s-LAG] Dang gia lap do tre mang %d ms truoc khi tra ve...%n", replicaId, delay);
                    Thread.sleep(delay);
                } catch (InterruptedException ignored) {}
            }

            Map<String, String> queryParams = parseQuery(exchange.getRequestURI().getQuery());
            String key = queryParams.get("key");

            if (key == null || key.trim().isEmpty()) {
                sendResponse(exchange, 400, "{\"error\": \"Tham so 'key' khong duoc de trong\"}");
                return;
            }

            KeyValueRecord record = localStorage.get(key);
            if (record == null) {
                Map<String, Object> notFound = new HashMap<>();
                notFound.put("found", false);
                notFound.put("key", key);
                notFound.put("replicaId", replicaId);
                notFound.put("nodeVersion", localStorage.getCurrentVersion());
                sendResponse(exchange, 404, gson.toJson(notFound));
                return;
            }

            Map<String, Object> result = new HashMap<>();
            result.put("found", true);
            result.put("key", record.getKey());
            result.put("value", record.getValue());
            result.put("version", record.getVersion());
            result.put("timestamp", record.getTimestamp());
            result.put("replicaId", replicaId);
            result.put("nodeVersion", localStorage.getCurrentVersion());

            System.out.printf("[%s-READ] Tra ve key='%s', value='%s', v=%d cho Client%n",
                    replicaId, record.getKey(), record.getValue(), record.getVersion());

            sendResponse(exchange, 200, gson.toJson(result));
        }
    }

    /**
     * POST /api/replicate
     * Nhận luồng replication từ Primary gửi sang.
     */
    private class ReplicateHandler implements HttpHandler {
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
            KeyValueRecord record;
            try {
                record = gson.fromJson(body, KeyValueRecord.class);
            } catch (Exception e) {
                sendResponse(exchange, 400, "{\"error\": \"Invalid JSON\"}");
                return;
            }

            if (record == null) {
                sendResponse(exchange, 400, "{\"error\": \"Empty record\"}");
                return;
            }

            // Ghi bản sao bền vững vào đĩa
            boolean applied = localStorage.applyRecord(record);

            Map<String, Object> ack = new HashMap<>();
            ack.put("status", applied ? "ACK" : "SKIPPED_STALE");
            ack.put("replicaId", replicaId);
            ack.put("appliedVersion", record.getVersion());
            ack.put("currentLocalVersion", localStorage.getCurrentVersion());

            System.out.printf("[%s-REPLICATION] Da nhan va ghi ban sao: key='%s', v=%d -> ACK Primary%n",
                    replicaId, record.getKey(), record.getVersion());

            sendResponse(exchange, 200, gson.toJson(ack));
        }
    }

    /**
     * GET /api/status
     * Báo cáo trạng thái Node cho Dashboard & Health check
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
            String currentState = (simulatedDelayMs.get() > 0) ? "SIMULATING_LAG" : state;

            NodeStatus status = new NodeStatus(
                    replicaId,
                    "BACKUP",
                    port,
                    currentState,
                    localStorage.getCurrentVersion(),
                    localStorage.getActiveKeysCount(),
                    simulatedDelayMs.get(),
                    uptime
            );

            sendResponse(exchange, 200, gson.toJson(status));
        }
    }

    /**
     * GET /api/data
     * Trả về toàn bộ dữ liệu đang lưu trong đĩa/bộ nhớ
     */
    private class DataDumpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            Map<String, Object> data = new HashMap<>();
            data.put("replicaId", replicaId);
            data.put("currentVersion", localStorage.getCurrentVersion());
            data.put("records", localStorage.getAllRecords());

            sendResponse(exchange, 200, gson.toJson(data));
        }
    }

    /**
     * POST /api/catchup
     * Kích hoạt thủ công quy trình bắt kịp dữ liệu
     */
    private class TriggerCatchUpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            boolean ok = catchUpEngine.performCatchUp();
            Map<String, Object> res = new HashMap<>();
            res.put("success", ok);
            res.put("replicaId", replicaId);
            res.put("currentVersion", localStorage.getCurrentVersion());
            sendResponse(exchange, 200, gson.toJson(res));
        }
    }

    /**
     * POST /api/admin/delay?ms=3000
     * Thiết lập độ trễ mô phỏng
     */
    private class SetDelayHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            Map<String, String> q = parseQuery(exchange.getRequestURI().getQuery());
            long ms = 0;
            if (q.containsKey("ms")) {
                try {
                    ms = Long.parseLong(q.get("ms"));
                } catch (Exception ignored) {}
            }
            simulatedDelayMs.set(ms);
            System.out.printf("[%s-ADMIN] Thiet lap simulatedDelayMs = %d ms%n", replicaId, ms);

            Map<String, Object> res = new HashMap<>();
            res.put("simulatedDelayMs", ms);
            res.put("message", "Da cap nhat do tre gia lap: " + ms + "ms");
            sendResponse(exchange, 200, gson.toJson(res));
        }
    }

    /**
     * POST /api/admin/clear
     * Xóa sạch dữ liệu trên đĩa
     */
    private class ClearDataHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }

            localStorage.clearAll();
            sendResponse(exchange, 200, "{\"status\": \"OK\", \"message\": \"Da xoa sach data\"}");
        }
    }

    /**
     * POST /api/admin/crash
     * Giả lập sập server (Crash simulation) để phục vụ bài test demo
     */
    private class SimulateCrashHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            sendResponse(exchange, 200, "{\"status\": \"CRASHED\", \"message\": \"Node dang tat dot ngot...\"}");
            System.out.printf("[CRASH-SIMULATION] !!! NODE %s BI TAT DOT NGOT (CRASHED) !!!%n", replicaId);
            new Thread(() -> {
                try { Thread.sleep(300); } catch (Exception ignored) {}
                stop();
            }).start();
        }
    }

    // ==========================================
    // Utility Methods
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
                } else {
                    result.put(URLDecoder.decode(pair, StandardCharsets.UTF_8.name()), "");
                }
            } catch (Exception ignored) {}
        }
        return result;
    }

    public LocalStorage getLocalStorage() {
        return localStorage;
    }

    public CatchUpEngine getCatchUpEngine() {
        return catchUpEngine;
    }

    // ==========================================
    // Main Method: Run independently from Terminal
    // Usage: java -cp ... com.phantan.backup.BackupServer [port] [replicaId] [primaryUrl]
    // Default: 8002 backup-1 http://localhost:8001
    // ==========================================
    public static void main(String[] args) {
        int port = 8002;
        String replicaId = "backup-1";
        String primaryUrl = "http://localhost:8001";
        String dataDir = "./data_backup_8002";

        if (args.length >= 1) {
            try { port = Integer.parseInt(args[0]); } catch (Exception ignored) {}
        }
        if (args.length >= 2) {
            replicaId = args[1];
        }
        if (args.length >= 3) {
            primaryUrl = args[2];
        }
        dataDir = "./data_" + replicaId + "_" + port;

        try {
            BackupServer server = new BackupServer(replicaId, port, dataDir, primaryUrl);
            server.start();
            System.out.println("Nhan Ctrl + C hoac kill tien trinh de thoat.");
        } catch (Exception e) {
            System.err.println("Loi khoi dong BackupServer: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
