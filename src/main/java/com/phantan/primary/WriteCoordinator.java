package com.phantan.primary;

import com.google.gson.Gson;
import com.phantan.common.KeyValueRecord;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * [MODULE BẠN 1 PHỤ TRÁCH]
 * WriteCoordinator - Điều phối ghi dữ liệu và nhân bản (Sync vs Async Replication).
 *
 * Lý thuyết phân tán (Slide Lect06 - Consistency and Replication):
 * 1. Chế độ Đồng bộ (Synchronous / Eager Replication):
 *    - Gửi bản sao sang các Backup.
 *    - Chờ tất cả (hoặc đa số Quorum) Backup phản hồi ACK thành công mới trả về kết quả cho Client.
 *    - Ưu điểm: Tính nhất quán mạnh (Strong Consistency).
 *    - Nhược điểm: Độ trễ (Latency) cao, phụ thuộc vào node chậm nhất.
 * 2. Chế độ Bất đồng bộ (Asynchronous / Lazy Replication):
 *    - Ghi vào Primary xong báo thành công ngay tức thì cho Client.
 *    - Đẩy bản sao ngầm trong luồng nền (Background thread).
 *    - Ưu điểm: Phản hồi cực nhanh (Low Latency).
 *    - Nhược điểm: Backup có thể bị trễ một khoảng thời gian (Replication Lag).
 */
public class WriteCoordinator {
    public enum Mode { SYNC, ASYNC }

    private volatile Mode currentMode = Mode.SYNC;
    private final List<String> replicaUrls;
    private final Gson gson;
    private final ExecutorService asyncPool;

    public WriteCoordinator(List<String> replicaUrls) {
        this.replicaUrls = new CopyOnWriteArrayList<>(replicaUrls);
        this.gson = new Gson();
        this.asyncPool = Executors.newFixedThreadPool(4);
    }

    public Mode getCurrentMode() {
        return currentMode;
    }

    public void setCurrentMode(Mode mode) {
        this.currentMode = mode;
        System.out.printf("[WRITE-COORDINATOR] Da chuyen che do replication sang: %s%n", mode);
    }

    /**
     * Điều phối nhân bản theo chế độ hiện tại (SYNC hoặc ASYNC)
     */
    public boolean replicate(KeyValueRecord record) {
        if (currentMode == Mode.SYNC) {
            return replicateSync(record);
        } else {
            replicateAsync(record);
            return true; // Báo thành công ngay cho Client
        }
    }

    /**
     * Đồng bộ: Chờ các Backup phản hồi ACK
     */
    private boolean replicateSync(KeyValueRecord record) {
        String json = gson.toJson(record);
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();

        for (String url : replicaUrls) {
            futures.add(CompletableFuture.supplyAsync(() -> sendReplication(url, json), asyncPool));
        }

        // Chờ tất cả Backup hoàn tất (hoặc timeout 2 giây)
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(2, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            System.err.println("[SYNC-WARN] Co replica chua kip ACK trong 2s: " + e.getMessage());
            return true; // Primary da ghi, van chap nhan
        }
    }

    /**
     * Bất đồng bộ: Đẩy ngầm không chặn Client
     */
    private void replicateAsync(KeyValueRecord record) {
        String json = gson.toJson(record);
        for (String url : replicaUrls) {
            asyncPool.submit(() -> sendReplication(url, json));
        }
    }

    private boolean sendReplication(String replicaUrl, String json) {
        try {
            URL url = URI.create(replicaUrl + "/api/replicate").toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1500);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }

            return conn.getResponseCode() == 200;
        } catch (Exception e) {
            // Node co the dang offline (sập node)
            return false;
        }
    }
}
