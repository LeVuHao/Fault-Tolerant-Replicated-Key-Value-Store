package com.phantan.common;

/**
 * NodeStatus - Thông tin trạng thái hoạt động của Node phục vụ giám sát,
 * hiển thị Dashboard và kiểm tra tính nhất quán.
 */
public class NodeStatus {
    private String nodeId;
    private String role;            // "PRIMARY" hoặc "BACKUP"
    private int port;
    private String state;           // "ONLINE", "CATCHING_UP", "SYNCHRONIZED", "CRASHED", "SIMULATING_LAG"
    private long currentVersion;
    private int totalKeys;
    private long simulatedDelayMs;
    private long uptimeSeconds;

    public NodeStatus() {
    }

    public NodeStatus(String nodeId, String role, int port, String state, long currentVersion, int totalKeys, long simulatedDelayMs, long uptimeSeconds) {
        this.nodeId = nodeId;
        this.role = role;
        this.port = port;
        this.state = state;
        this.currentVersion = currentVersion;
        this.totalKeys = totalKeys;
        this.simulatedDelayMs = simulatedDelayMs;
        this.uptimeSeconds = uptimeSeconds;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(long currentVersion) {
        this.currentVersion = currentVersion;
    }

    public int getTotalKeys() {
        return totalKeys;
    }

    public void setTotalKeys(int totalKeys) {
        this.totalKeys = totalKeys;
    }

    public long getSimulatedDelayMs() {
        return simulatedDelayMs;
    }

    public void setSimulatedDelayMs(long simulatedDelayMs) {
        this.simulatedDelayMs = simulatedDelayMs;
    }

    public long getUptimeSeconds() {
        return uptimeSeconds;
    }

    public void setUptimeSeconds(long uptimeSeconds) {
        this.uptimeSeconds = uptimeSeconds;
    }

    @Override
    public String toString() {
        return String.format("NodeStatus[%s, role=%s, port=%d, state=%s, v=%d, keys=%d, lag=%dms]",
                nodeId, role, port, state, currentVersion, totalKeys, simulatedDelayMs);
    }
}
