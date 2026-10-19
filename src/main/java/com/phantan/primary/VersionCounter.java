package com.phantan.primary;

import java.util.concurrent.atomic.AtomicLong;

/**
 * [MODULE BẠN 1 PHỤ TRÁCH]
 * VersionCounter - Bộ cấp phát số phiên bản tăng đơn điệu (Monotonic Version Counter).
 *
 * Lý thuyết phân tán (Tanenbaum Chương 6 & Slide Lect06):
 * - Trong hệ thống phân tán, đồng hồ vật lý (Physical Clock) bị lệch (Clock Skew/Drift),
 *   không thể dùng System.currentTimeMillis() để xác định thứ tự.
 * - Ta dùng bộ đếm số nguyên v1, v2, v3... để bảo đảm Total Ordering (Thứ tự toàn phần)
 *   cho tất cả các thao tác cập nhật trên cụm.
 */
public class VersionCounter {
    private final AtomicLong counter;

    public VersionCounter() {
        this.counter = new AtomicLong(0);
    }

    public VersionCounter(long initialVersion) {
        this.counter = new AtomicLong(initialVersion);
    }

    public long nextVersion() {
        return counter.incrementAndGet();
    }

    public long getCurrentVersion() {
        return counter.get();
    }

    public void reset() {
        counter.set(0);
    }
}
