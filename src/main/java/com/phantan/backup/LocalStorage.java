package com.phantan.backup;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.phantan.common.KeyValueRecord;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LocalStorage - Quản lý lưu trữ bền vững (Local Persistent Key-Value Storage) cho Backup Replica.
 *
 * Các nguyên lý phân tán được cài đặt:
 * 1. Durability (Độ bền vững - Tanenbaum Chapter 8): Dữ liệu được lưu trữ xuống ổ đĩa bền vững
 *    dưới dạng Snapshot JSON và Write-Ahead Log (WAL).
 * 2. Crash Recovery (Phục hồi sau sự cố): Khi node bị tắt đột ngột (crash/kill) rồi khởi động lại,
 *    node tự nạp lại toàn bộ dữ liệu từ đĩa và khôi phục số version hiện tại.
 * 3. Atomic Write (Ghi đĩa nguyên tử): Sử dụng cơ chế ghi file tạm (.tmp) rồi Atomic Move để
 *    chống file corruption khi sập nguồn điện giữa lúc đang ghi.
 * 4. Idempotency & Monotonic Version Ordering: Đảm bảo chỉ áp dụng bản ghi mới hơn hoặc bằng,
 *    không bị ghi đè bởi bản ghi cũ lạc hậu.
 */
public class LocalStorage {
    private final String dataDir;
    private final Path storeFilePath;
    private final Path walFilePath;
    private final Gson gson;

    // Bộ nhớ RAM phục vụ đọc nhanh O(1)
    private final Map<String, KeyValueRecord> memTable;
    // Phiên bản dữ liệu cao nhất hiện tại của Node
    private final AtomicLong currentVersion;
    // Khóa đọc/ghi để an toàn đa luồng
    private final ReentrantReadWriteLock rwLock;

    public LocalStorage(String dataDir) {
        this.dataDir = dataDir;
        this.storeFilePath = Paths.get(dataDir, "store.json");
        this.walFilePath = Paths.get(dataDir, "wal.log");
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.memTable = new ConcurrentHashMap<>();
        this.currentVersion = new AtomicLong(0);
        this.rwLock = new ReentrantReadWriteLock();

        // Khởi tạo thư mục lưu trữ nếu chưa có
        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (IOException e) {
            System.err.println("[PERSISTENCE-ERROR] Không thể tạo thư mục dữ liệu: " + e.getMessage());
        }

        // Tự động khôi phục dữ liệu từ đĩa cứng khi khởi động (Crash Recovery)
        loadFromDisk();
    }

    /**
     * Khôi phục toàn bộ trạng thái từ đĩa cứng (Snapshot + WAL)
     */
    public synchronized void loadFromDisk() {
        rwLock.writeLock().lock();
        try {
            memTable.clear();
            currentVersion.set(0);

            // 1. Nạp Snapshot từ store.json nếu tồn tại
            if (Files.exists(storeFilePath)) {
                try (Reader reader = Files.newBufferedReader(storeFilePath, StandardCharsets.UTF_8)) {
                    Type type = new TypeToken<Map<String, KeyValueRecord>>() {}.getType();
                    Map<String, KeyValueRecord> loaded = gson.fromJson(reader, type);
                    if (loaded != null) {
                        for (Map.Entry<String, KeyValueRecord> entry : loaded.entrySet()) {
                            memTable.put(entry.getKey(), entry.getValue());
                            if (entry.getValue().getVersion() > currentVersion.get()) {
                                currentVersion.set(entry.getValue().getVersion());
                            }
                        }
                    }
                    System.out.printf("[PERSISTENCE] [INIT] Da nap thanh cong %d keys tu %s. Version hien tai: v%d%n",
                            memTable.size(), storeFilePath.getFileName(), currentVersion.get());
                } catch (Exception e) {
                    System.err.println("[PERSISTENCE-WARN] Loi khi doc file store.json: " + e.getMessage());
                }
            } else {
                System.out.printf("[PERSISTENCE] [INIT] Chua co file snapshot cu (%s). Khoi tao kho luu tru moi tinh (v0).%n",
                        storeFilePath.getFileName());
            }

            // 2. Replay Write-Ahead Log (WAL) nếu có thêm các bản ghi sau snapshot
            if (Files.exists(walFilePath)) {
                int walReplayed = 0;
                try (BufferedReader br = Files.newBufferedReader(walFilePath, StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        try {
                            KeyValueRecord record = gson.fromJson(line, KeyValueRecord.class);
                            if (record != null) {
                                KeyValueRecord existing = memTable.get(record.getKey());
                                if (existing == null || record.getVersion() >= existing.getVersion()) {
                                    memTable.put(record.getKey(), record);
                                    if (record.getVersion() > currentVersion.get()) {
                                        currentVersion.set(record.getVersion());
                                    }
                                    walReplayed++;
                                }
                            }
                        } catch (Exception parseEx) {
                            // Bo qua dong loi trong WAL neu bi cat giua chung
                        }
                    }
                }
                if (walReplayed > 0) {
                    System.out.printf("[PERSISTENCE] [WAL-REPLAY] Da replay %d dong tu %s. Version sau cung: v%d%n",
                            walReplayed, walFilePath.getFileName(), currentVersion.get());
                    // Flush snapshot de nén WAL
                    persistSnapshotToDisk();
                }
            }
        } catch (Exception e) {
            System.err.println("[PERSISTENCE-ERROR] Loi trong qua trinh loadFromDisk: " + e.getMessage());
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Áp dụng một bản ghi mới từ luồng Replication của Primary.
     * Kiểm tra tính Idempotent và cập nhật đĩa.
     */
    public boolean applyRecord(KeyValueRecord record) {
        if (record == null) return false;

        rwLock.writeLock().lock();
        try {
            // Kiem tra thu tu version: neu version nho hon ban ghi da co thi tu choi ghi de
            KeyValueRecord existing = memTable.get(record.getKey());
            if (existing != null && record.getVersion() < existing.getVersion()) {
                System.out.printf("[PERSISTENCE-STALE] Bo qua ban ghi loi thoi key='%s' (v%d < v%d hien tai)%n",
                        record.getKey(), record.getVersion(), existing.getVersion());
                return false;
            }

            // Cap nhat memTable
            memTable.put(record.getKey(), record);

            // Cap nhat Version don dieu tang dan cua Node
            if (record.getVersion() > currentVersion.get()) {
                currentVersion.set(record.getVersion());
            }

            // Ghi append vao WAL va cap nhat Snapshot
            appendRecordToWal(record);
            persistSnapshotToDisk();

            System.out.printf("[PERSISTENCE-OK] Da luu ben vung key='%s', value='%s', version=v%d (isDeleted=%s)%n",
                    record.getKey(), record.getValue(), record.getVersion(), record.isDeleted());
            return true;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Áp dụng một chuỗi bản ghi bù (Batch updates) nhận từ Catch-Up Recovery.
     * Toàn bộ danh sách được cập nhật và lưu đĩa an toàn.
     */
    public int applyBatch(List<KeyValueRecord> records) {
        if (records == null || records.isEmpty()) return 0;

        rwLock.writeLock().lock();
        try {
            int appliedCount = 0;
            // Sap xep theo version tang dan de dam bao Total Order khi replay
            List<KeyValueRecord> sorted = new ArrayList<>(records);
            sorted.sort(Comparator.comparingLong(KeyValueRecord::getVersion));

            for (KeyValueRecord rec : sorted) {
                KeyValueRecord existing = memTable.get(rec.getKey());
                if (existing == null || rec.getVersion() >= existing.getVersion()) {
                    memTable.put(rec.getKey(), rec);
                    if (rec.getVersion() > currentVersion.get()) {
                        currentVersion.set(rec.getVersion());
                    }
                    appendRecordToWal(rec);
                    appliedCount++;
                }
            }

            // Ghi lai Snapshot file JSON day du tren dia
            persistSnapshotToDisk();

            System.out.printf("[PERSISTENCE-BATCH] Da apply thanh cong %d/%d ban ghi catch-up vao dia. Version hien tai: v%d%n",
                    appliedCount, records.size(), currentVersion.get());
            return appliedCount;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Đọc một bản ghi theo key
     */
    public KeyValueRecord get(String key) {
        rwLock.readLock().lock();
        try {
            KeyValueRecord record = memTable.get(key);
            if (record != null && record.isDeleted()) {
                // Tombstone: Da bi xoa
                return null;
            }
            return record;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Lấy version hiện tại của node
     */
    public long getCurrentVersion() {
        return currentVersion.get();
    }

    /**
     * Lấy tổng số key còn hiệu lực (không tính key bị tombstone xóa)
     */
    public int getActiveKeysCount() {
        rwLock.readLock().lock();
        try {
            int count = 0;
            for (KeyValueRecord rec : memTable.values()) {
                if (!rec.isDeleted()) count++;
            }
            return count;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Lấy toàn bộ dữ liệu đang lưu trong node (dùng cho API và hiển thị Web Dashboard)
     */
    public Map<String, KeyValueRecord> getAllRecords() {
        rwLock.readLock().lock();
        try {
            return new HashMap<>(memTable);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Xóa sạch dữ liệu (phục vụ reset khi bắt đầu bài test mới)
     */
    public void clearAll() {
        rwLock.writeLock().lock();
        try {
            memTable.clear();
            currentVersion.set(0);
            try {
                Files.deleteIfExists(storeFilePath);
                Files.deleteIfExists(walFilePath);
            } catch (IOException e) {
                System.err.println("[PERSISTENCE-WARN] Khong the xoa file cu: " + e.getMessage());
            }
            System.out.println("[PERSISTENCE] Da clear toan bo du lieu va reset ve v0.");
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public String getDataDir() {
        return dataDir;
    }

    // ==========================================
    // Internal Helper Methods: Disk I/O & WAL
    // ==========================================

    private void appendRecordToWal(KeyValueRecord record) {
        try (BufferedWriter bw = Files.newBufferedWriter(walFilePath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            bw.write(gson.toJson(record));
            bw.newLine();
            bw.flush();
        } catch (IOException e) {
            System.err.println("[WAL-ERROR] Khong the ghi vao WAL: " + e.getMessage());
        }
    }

    /**
     * Ghi đĩa an toàn (Atomic File Replacement):
     * Ghi ra store.json.tmp trước, sau đó Atomic Move sang store.json.
     * Đảm bảo nếu bị kill bất tử giữa chừng thì file store.json vẫn toàn vẹn.
     */
    private void persistSnapshotToDisk() {
        Path tempPath = Paths.get(dataDir, "store.json.tmp");
        try {
            String json = gson.toJson(memTable);
            Files.writeString(tempPath, json, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            // Atomic Move
            try {
                Files.move(tempPath, storeFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                // Fallback nếu filesystem không hỗ trợ atomic move
                Files.move(tempPath, storeFilePath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            System.err.println("[PERSISTENCE-ERROR] Khong the luu snapshot xuong dia: " + e.getMessage());
        }
    }
}
