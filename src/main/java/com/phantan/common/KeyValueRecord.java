package com.phantan.common;

import java.util.Objects;

/**
 * KeyValueRecord - Đại diện cho một bản ghi Key-Value trong hệ thống phân tán.
 * Bám sát theo:
 * - Slide Lect06: Consistency and Replication
 * - Sách giáo trình Tanenbaum Chapter 7 (Sec 7.1 - 7.5)
 *
 * Mỗi bản ghi có:
 * - key: Khóa định danh
 * - value: Giá trị lưu trữ
 * - version: Số hiệu phiên bản tăng đơn điệu (Monotonic Version Counter)
 *            Được cấp phát bởi Primary Node để đảm bảo Total Ordering.
 * - timestamp: Thời điểm ghi (epoch milliseconds)
 * - isDeleted: Đánh dấu xóa (Tombstone pattern)
 */
public class KeyValueRecord {
    private String key;
    private String value;
    private long version;
    private long timestamp;
    private boolean isDeleted;

    public KeyValueRecord() {
    }

    public KeyValueRecord(String key, String value, long version) {
        this(key, value, version, System.currentTimeMillis(), false);
    }

    public KeyValueRecord(String key, String value, long version, long timestamp, boolean isDeleted) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.timestamp = timestamp;
        this.isDeleted = isDeleted;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValueRecord that = (KeyValueRecord) o;
        return version == that.version &&
                timestamp == that.timestamp &&
                isDeleted == that.isDeleted &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, version, timestamp, isDeleted);
    }

    @Override
    public String toString() {
        return String.format("KeyValueRecord{key='%s', value='%s', v=%d, deleted=%s, ts=%d}",
                key, value, version, isDeleted, timestamp);
    }
}
