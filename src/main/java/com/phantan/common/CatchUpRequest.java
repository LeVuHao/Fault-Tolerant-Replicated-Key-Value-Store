package com.phantan.common;

/**
 * CatchUpRequest - Gói tin yêu cầu bắt kịp dữ liệu (State Synchronization / Anti-Entropy)
 * Gửi từ Backup Replica tới Primary Node khi vừa khởi động lại (Rejoining after crash)
 * hoặc khi phát hiện có lỗ hổng (gap) trong luồng version.
 */
public class CatchUpRequest {
    private String replicaId;     // Định danh của Backup (ví dụ: "backup-1", "backup-2")
    private long fromVersion;     // Version hiện tại của Backup (yêu cầu gửi các bản ghi từ version này + 1)
    private long targetVersion;   // Version mong muốn đạt đến (0 nghĩa là lấy tới mới nhất)

    public CatchUpRequest() {
    }

    public CatchUpRequest(String replicaId, long fromVersion) {
        this(replicaId, fromVersion, 0);
    }

    public CatchUpRequest(String replicaId, long fromVersion, long targetVersion) {
        this.replicaId = replicaId;
        this.fromVersion = fromVersion;
        this.targetVersion = targetVersion;
    }

    public String getReplicaId() {
        return replicaId;
    }

    public void setReplicaId(String replicaId) {
        this.replicaId = replicaId;
    }

    public long getFromVersion() {
        return fromVersion;
    }

    public void setFromVersion(long fromVersion) {
        this.fromVersion = fromVersion;
    }

    public long getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(long targetVersion) {
        this.targetVersion = targetVersion;
    }

    @Override
    public String toString() {
        return String.format("CatchUpRequest{replicaId='%s', fromVersion=%d, targetVersion=%d}",
                replicaId, fromVersion, targetVersion);
    }
}
