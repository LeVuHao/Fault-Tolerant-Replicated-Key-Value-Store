package com.phantan.client_proxy;

import com.google.gson.Gson;
import com.phantan.common.KeyValueRecord;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * [MODULE BẠN 3 PHỤ TRÁCH]
 * SmartClientProxy - Middleware Proxy thông minh bám sát Hình 3-10 trang 107 sách Tanenbaum.
 *
 * Tính năng:
 * 1. Duy trì Context phiên làm việc của Client:
 *    - lastWriteVersions: lưu version lần ghi gần nhất do chính Client này thực hiện.
 *    - lastReadVersions: lưu version lần đọc gần nhất của Client.
 * 2. Đảm bảo bảo chứng Read-Your-Writes:
 *    - Nếu Backup trả về version cũ hơn lần ghi gần nhất (do mạng trễ), Proxy từ chối
 *      và tự động fallback sang Primary để đọc đúng giá trị mới nhất.
 * 3. Đảm bảo bảo chứng Monotonic Reads:
 *    - Không cho phép đọc lùi về version cũ.
 * 4. Tự động Failover (Chuyển vùng khi sự cố):
 *    - Nếu node đang đọc bị timeout/mất kết nối, Proxy tự động chuyển sang replica khác.
 */
public class SmartClientProxy {
    private final String primaryUrl;
    private final List<String> backupUrls;
    private final Map<String, Long> lastWriteVersions;
    private final Map<String, Long> lastReadVersions;
    private final Gson gson;
    private int currentBackupIndex = 0;

    public SmartClientProxy(String primaryUrl, List<String> backupUrls) {
        this.primaryUrl = primaryUrl;
        this.backupUrls = new ArrayList<>(backupUrls);
        this.lastWriteVersions = new ConcurrentHashMap<>();
        this.lastReadVersions = new ConcurrentHashMap<>();
        this.gson = new Gson();
    }

    /**
     * Ghi dữ liệu: Luôn gửi đến Primary Node
     */
    public KeyValueRecord put(String key, String value) throws IOException {
        URL url = URI.create(primaryUrl + "/api/put").toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        Map<String, String> body = Map.of("key", key, "value", value);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(gson.toJson(body).getBytes(StandardCharsets.UTF_8));
        }

        if (conn.getResponseCode() == 200) {
            String respStr = readStream(conn.getInputStream());
            Map resp = gson.fromJson(respStr, Map.class);
            long version = ((Number) resp.get("version")).longValue();

            // Cập nhật ngữ cảnh phiên Client: nhớ version ghi gần nhất
            lastWriteVersions.put(key, version);
            System.out.printf("[PROXY-WRITE] Client ghi '%s' thanh cong tai v%d. Session Context updated!%n", key, version);

            return new KeyValueRecord(key, value, version);
        }
        throw new IOException("Loi ghi Primary: HTTP " + conn.getResponseCode());
    }

    /**
     * Đọc dữ liệu: Ưu tiên đọc từ Backup Replicas, kiểm tra bảo chứng nhất quán
     */
    public KeyValueRecord get(String key) throws IOException {
        long lastWriteV = lastWriteVersions.getOrDefault(key, 0L);
        long lastReadV = lastReadVersions.getOrDefault(key, 0L);

        // Thử đọc từ Backup hiện tại
        for (int attempts = 0; attempts < backupUrls.size(); attempts++) {
            String backupUrl = getNextBackupUrl();
            try {
                KeyValueRecord record = fetchFromNode(backupUrl, key);
                if (record != null) {
                    // 1. Kiểm tra vi phạm Read-Your-Writes
                    if (!ConsistencyValidator.checkReadYourWrites(key, record.getVersion(), lastWriteV)) {
                        System.out.printf("[PROXY-VIOLATION] READ-YOUR-WRITES VIOLATED tai %s! (Expected >= v%d, got v%d)%n",
                                backupUrl, lastWriteV, record.getVersion());
                        System.out.println("                  -> Proxy tu dong fallback sang Primary de lay du lieu moi nhat!");
                        return fetchFromNode(primaryUrl, key);
                    }

                    // 2. Kiểm tra vi phạm Monotonic Reads
                    if (!ConsistencyValidator.checkMonotonicReads(key, record.getVersion(), lastReadV)) {
                        System.out.printf("[PROXY-VIOLATION] MONOTONIC-READS VIOLATED tai %s! (Expected >= v%d, got v%d)%n",
                                backupUrl, lastReadV, record.getVersion());
                        return fetchFromNode(primaryUrl, key);
                    }

                    // Thỏa mãn tính nhất quán! Cập nhật ngữ cảnh đọc
                    lastReadVersions.put(key, record.getVersion());
                    System.out.printf("[PROXY-READ-OK] Doc tu %s thanh cong: key='%s', val='%s', v=%d%n",
                            backupUrl, key, record.getValue(), record.getVersion());
                    return record;
                }
            } catch (Exception e) {
                System.out.printf("[PROXY-FAILOVER] Node %s timed out hoac loi: %s. Dang failover sang node khac...%n",
                        backupUrl, e.getMessage());
            }
        }

        // Nếu tất cả Backup đều lỗi, failover sang Primary
        System.out.println("[PROXY-FAILOVER] Tat ca Backup deu offline! Chuyen huong sang Primary.");
        return fetchFromNode(primaryUrl, key);
    }

    private synchronized String getNextBackupUrl() {
        if (backupUrls.isEmpty()) return primaryUrl;
        String url = backupUrls.get(currentBackupIndex);
        currentBackupIndex = (currentBackupIndex + 1) % backupUrls.size();
        return url;
    }

    private KeyValueRecord fetchFromNode(String nodeUrl, String key) throws IOException {
        URL url = URI.create(nodeUrl + "/api/get?key=" + key).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(1500);
        conn.setReadTimeout(2000);

        if (conn.getResponseCode() == 200) {
            String respStr = readStream(conn.getInputStream());
            Map resp = gson.fromJson(respStr, Map.class);
            String val = (String) resp.get("value");
            long version = ((Number) resp.get("version")).longValue();
            return new KeyValueRecord(key, val, version);
        } else if (conn.getResponseCode() == 404) {
            return null;
        }
        throw new IOException("HTTP " + conn.getResponseCode());
    }

    private String readStream(InputStream is) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
            return sb.toString();
        }
    }
}
