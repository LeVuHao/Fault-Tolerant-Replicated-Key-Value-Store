package com.phantan.common;

import java.util.ArrayList;
import java.util.List;

/**
 * CatchUpResponse - Gói tin phản hồi từ Primary Node chứa các bản ghi bị lỡ
 * để Backup Node bù đắp dữ liệu (Replay Log).
 */
public class CatchUpResponse {
    private String replicaId;
    private long fromVersion;
    private long toVersion;
    private List<KeyValueRecord> missingRecords;
    private boolean success;
    private String message;

    public CatchUpResponse() {
        this.missingRecords = new ArrayList<>();
    }

    public CatchUpResponse(String replicaId, long fromVersion, long toVersion, List<KeyValueRecord> missingRecords) {
        this.replicaId = replicaId;
        this.fromVersion = fromVersion;
        this.toVersion = toVersion;
        this.missingRecords = (missingRecords != null) ? missingRecords : new ArrayList<>();
        this.success = true;
        this.message = "OK";
    }

    public static CatchUpResponse failure(String replicaId, String message) {
        CatchUpResponse resp = new CatchUpResponse();
        resp.setReplicaId(replicaId);
        resp.setSuccess(false);
        resp.setMessage(message);
        return resp;
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

    public long getToVersion() {
        return toVersion;
    }

    public void setToVersion(long toVersion) {
        this.toVersion = toVersion;
    }

    public List<KeyValueRecord> getMissingRecords() {
        return missingRecords;
    }

    public void setMissingRecords(List<KeyValueRecord> missingRecords) {
        this.missingRecords = missingRecords;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return String.format("CatchUpResponse{replicaId='%s', from=%d, to=%d, recordsCount=%d, success=%s}",
                replicaId, fromVersion, toVersion, (missingRecords != null ? missingRecords.size() : 0), success);
    }
}
