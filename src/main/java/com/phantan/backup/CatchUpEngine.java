package com.phantan.backup;

import com.google.gson.Gson;
import com.phantan.common.CatchUpRequest;
import com.phantan.common.CatchUpResponse;
import com.phantan.common.KeyValueRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * CatchUpEngine - Động cơ tự phục hồi dữ liệu khi node khởi động lại sau crash.
 *
 * Căn cứ lý thuyết phân tán:
 * 1. Anti-Entropy Protocol (Tanenbaum Sec 7.4 - Quản lý bản sao):
 *    Cơ chế đồng bộ trạng thái giữa các bản sao để phát hiện và bù đắp các cập nhật bị thiếu.
 * 2. Crash-Recovery via Log Replay (Tanenbaum Chapter 8 - Fault Tolerance):
 *    Node sau khi sống lại sẽ xác định checkpoint cục bộ (local version) và xin Primary
 *    danh sách Delta Updates để replay tuần tự vào kho dữ liệu.
 */
public class CatchUpEngine {
    private final String replicaId;
    private final LocalStorage localStorage;
    private final String primaryUrl;
    private final Gson gson;
    private final ScheduledExecutorService scheduler;
    private Consumer<String> stateChangeListener;

    public CatchUpEngine(String replicaId, LocalStorage localStorage, String primaryUrl) {
        this.replicaId = replicaId;
        this.localStorage = localStorage;
        this.primaryUrl = primaryUrl.endsWith("/") ? primaryUrl.substring(0, primaryUrl.length() - 1) : primaryUrl;
        this.gson = new Gson();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "CatchUpScheduler-" + replicaId);
            t.setDaemon(true);
            return t;
        });
    }

    public void setStateChangeListener(Consumer<String> listener) {
        this.stateChangeListener = listener;
    }

    private void notifyState(String newState) {
        if (stateChangeListener != null) {
            stateChangeListener.accept(newState);
        }
    }

    /**
     * Thực hiện quy trình Catch-Up với Primary Node (Trọng tâm Demo)
     *
     * Các bước thực hiện:
     * 1. Đọc version cục bộ hiện có trên đĩa cứng (ví dụ v5).
     * 2. Gửi CatchUpRequest đến Primary: "Tôi là replicaId, version của tôi là v5, hãy gửi bù từ v6 trở đi".
     * 3. Nhận CatchUpResponse chứa danh sách Missing Records.
     * 4. Replay các bản ghi vào LocalStorage và lưu đĩa.
     * 5. Đổi trạng thái sang SYNCHRONIZED.
     */
    public synchronized boolean performCatchUp() {
        long localVersion = localStorage.getCurrentVersion();
        System.out.println("======================================================================");
        System.out.printf("[CATCH-UP] [START] Node '%s' bat dau quy trinh Catch-up voi Primary (%s)%n",
                replicaId, primaryUrl);
        System.out.printf("[CATCH-UP] Trạng thái hiện tại trên đĩa cục bộ: version = v%d (Tổng keys: %d)%n",
                localVersion, localStorage.getActiveKeysCount());

        notifyState("CATCHING_UP");

        try {
            // Gửi yêu cầu xin bù dữ liệu tới Primary qua HTTP POST /api/catchup
            URL url = URI.create(primaryUrl + "/api/catchup").toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            conn.setRequestProperty("Accept", "application/json");
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(5000);
            conn.setDoOutput(true);

            CatchUpRequest req = new CatchUpRequest(replicaId, localVersion);
            String reqBody = gson.toJson(req);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(reqBody.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                System.err.printf("[CATCH-UP-ERROR] Primary tra ve ma loi HTTP: %d%n", responseCode);
                notifyState("ONLINE");
                return false;
            }

            // Đọc kết quả từ Primary
            StringBuilder responseBuilder = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    responseBuilder.append(line);
                }
            }

            CatchUpResponse resp = gson.fromJson(responseBuilder.toString(), CatchUpResponse.class);
            if (resp == null || !resp.isSuccess()) {
                String msg = (resp != null) ? resp.getMessage() : "Response null";
                System.err.printf("[CATCH-UP-WARN] Primary thong bao khong the sync: %s%n", msg);
                notifyState("ONLINE");
                return false;
            }

            List<KeyValueRecord> missingRecords = resp.getMissingRecords();
            long remoteTargetVersion = resp.getToVersion();

            if (missingRecords == null || missingRecords.isEmpty()) {
                if (remoteTargetVersion <= localVersion) {
                    System.out.printf("[CATCH-UP-DONE] Node da o trang thai moi nhat (v%d). Khong co ban ghi nao bi thieu!%n",
                            localVersion);
                } else {
                    System.out.printf("[CATCH-UP-INFO] Primary o v%d nhung khong co ban ghi lech.%n", remoteTargetVersion);
                }
                notifyState("SYNCHRONIZED");
                System.out.println("======================================================================");
                return true;
            }

            // Có bản ghi bị thiếu -> Tiến hành Replay Delta Updates
            System.out.printf("[CATCH-UP] [DETECTED GAP] Phat hien lech du lieu!%n");
            System.out.printf("           - Local Version : v%d%n", localVersion);
            System.out.printf("           - Primary Target: v%d%n", remoteTargetVersion);
            System.out.printf("           - So ban ghi bu : %d ban ghi%n", missingRecords.size());

            for (KeyValueRecord r : missingRecords) {
                System.out.printf("           -> Replaying: key='%s', val='%s', v%d%n",
                        r.getKey(), r.getValue(), r.getVersion());
            }

            // Ghi batch vào đĩa cứng
            int applied = localStorage.applyBatch(missingRecords);

            long updatedVersion = localStorage.getCurrentVersion();
            System.out.printf("[CATCH-UP] [SUCCESS] Da bu thanh cong %d ban ghi! Version hien tai da dat: v%d%n",
                    applied, updatedVersion);
            System.out.printf("[CATCH-UP] Trang thai Node '%s': [SYNCHRONIZED]%n", replicaId);
            System.out.println("======================================================================");

            notifyState("SYNCHRONIZED");
            return true;
        } catch (Exception e) {
            System.err.printf("[CATCH-UP-ERROR] Khong the ket noi den Primary tai %s: %s%n", primaryUrl, e.getMessage());
            notifyState("ONLINE");
            System.out.println("======================================================================");
            return false;
        }
    }

    /**
     * Bật luồng đồng bộ Anti-Entropy định kỳ trong nền (Background Heartbeat / Sync)
     */
    public void startPeriodicSync(int intervalSeconds) {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                performCatchUp();
            } catch (Exception e) {
                // Log silently
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        System.out.printf("[CATCH-UP] Da khoi chay Anti-Entropy Sync ngam moi %d giay.%n", intervalSeconds);
    }

    public void stop() {
        scheduler.shutdown();
    }
}
