# Hệ Thống Lưu Trữ Phân Tán Khóa - Giá Trị Chịu Lỗi (Track C)
## Fault-Tolerant Replicated Key-Value Store

> **Học phần:** Các Hệ Thống Phân Tán (Distributed Systems)  
> **Căn cứ lý thuyết:** Giáo trình *Distributed Systems: Principles and Paradigms* (Andrew S. Tanenbaum, Maarten van Steen) & Bài giảng Slide Lect02, Lect06, Lect07, Lect08.

---

## 🏛️ 1. Kiến Trúc Tổng Thể Cụm Phân Tán

Hệ thống hoạt động theo mô hình **Primary-Backup Replication** với 1 Node Chủ (Primary), 2 Node Bản sao (Backup Replicas), 1 Thư viện Smart Client Proxy và 1 Web Dashboard quan sát trực quan:

```mermaid
graph TD
    Client["Smart Client Proxy<br/>(Student 3 - Port 8000/Lib)"]
    Primary["Primary / Coordinator<br/>(Student 1 - Port 8001)"]
    Backup1["Backup Replica 1<br/>(Port 8002)"]
    Backup2["Backup Replica 2<br/>(Student 2 Hảo - Port 8003)"]
    Dashboard["Web Dashboard UI<br/>(dashboard/index.html)"]

    Client -->|"1. PUT / DELETE"| Primary
    Client -.->|"4. GET (Read-Your-Writes)"| Backup1
    Client -.->|"4. GET (Read-Your-Writes)"| Backup2
    Primary -->|"2. Replicate Stream (Sync/Async)"| Backup1
    Primary -->|"2. Replicate Stream (Sync/Async)"| Backup2
    Backup2 ==="3. Anti-Entropy Catch-Up Recovery"===> Primary
    Dashboard -.->|"Poll status & live data"| Primary
    Dashboard -.->|"Poll status & live data"| Backup1
    Dashboard -.->|"Poll status & live data"| Backup2
```

---

## 👥 2. Phân Chia Module Cho 3 Thành Viên

| Thành viên | Module phụ trách | Trách nhiệm chính |
|---|---|---|
| **Student 1** | `com.phantan.primary` | **Primary Node & Write Protocol:** Tiếp nhận PUT/DELETE, bộ đếm Monotonic Version Counter ($v_1, v_2, v_3\dots$), điều phối ghi Đồng bộ (Sync) / Bất đồng bộ (Async), Write-Ahead Log (WAL). |
| **Student 2 (Hảo)** | `com.phantan.backup` | **Backup Replicas & Catch-Up Recovery:** Quản lý lưu trữ đĩa bền vững (JSON Snapshot + WAL + Atomic write), tiếp nhận luồng replication, phục vụ GET, tự động phục hồi dữ liệu bị lỡ khi sập node rồi khởi động lại. |
| **Student 3** | `com.phantan.client_proxy` | **Smart Client Proxy & Client-Centric Consistency:** Middleware bám sát Hình 3-10 Tanenbaum, lưu Session Context, bảo chứng Read-Your-Writes và Monotonic Reads, tự động Failover khi node chết. |
| **Dùng chung** | `com.phantan.common`<br>`dashboard/` | Data Models (`KeyValueRecord`, `CatchUpRequest`, `CatchUpResponse`, `NodeStatus`) và Web Dashboard trực quan. |

---

## 🚀 3. Hướng Dẫn Cài Đặt & Khởi Chạy

### Yêu cầu môi trường
* **Java:** JDK 17 trở lên
* **Maven:** 3.8+

### Cách 1: Khởi động 1 click (Dành cho Demo)
Nhấp đúp chuột vào file:
```cmd
run_all_nodes.bat
```
Hệ thống sẽ tự động bật 3 terminal tương ứng với 3 Node (8001, 8002, 8003) và mở Web Dashboard trên trình duyệt.

### Cách 2: Khởi động từng Node bằng Maven
```bash
# Biên dịch toàn bộ project
mvn clean compile

# Chạy Primary (Port 8001)
mvn exec:java -Dexec.mainClass="com.phantan.primary.PrimaryServer"

# Chạy Backup 1 (Port 8002)
mvn exec:java -Dexec.mainClass="com.phantan.backup.BackupServer" -Dexec.args="8002 backup-1 http://localhost:8001"

# Chạy Backup 2 (Port 8003 - Node của Hảo)
mvn exec:java -Dexec.mainClass="com.phantan.backup.BackupServer" -Dexec.args="8003 backup-2 http://localhost:8001"
```

### Chạy kiểm thử tự động (Unit & Integration Test)
```bash
mvn test
```
*(Kịch bản kiểm thử tự động kiểm chứng Crash & Catch-up recovery sẽ chạy và in kết quả `BUILD SUCCESS`)*.

---

## 📡 4. Chuẩn Giao Thức API Giữa Các Node

### Primary Node (Port 8001)
* `POST /api/put`: Nhận ghi key-value: `{ "key": "user:1", "value": "Alice" }`
* `POST /api/delete`: Nhận xóa key: `{ "key": "user:1" }`
* `POST /api/catchup`: Nhận `CatchUpRequest`, trích xuất WAL gửi bù các bản ghi cho Backup
* `GET /api/status`: Lấy trạng thái node, version hiện tại, tổng bản ghi
* `GET /api/wal`: Xuất toàn bộ Write-Ahead Log
* `POST /api/mode?mode=SYNC|ASYNC`: Đổi chế độ replication

### Backup Nodes (Port 8002, 8003)
* `GET /api/get?key=...`: Phục vụ đọc key kèm version
* `POST /api/replicate`: Nhận bản ghi nhân bản từ Primary
* `GET /api/status`: Lấy trạng thái node (`ONLINE`, `CATCHING_UP`, `SYNCHRONIZED`)
* `GET /api/data`: Lấy toàn bộ dữ liệu đang lưu trong bộ nhớ và đĩa
* `POST /api/catchup`: Kích hoạt thủ công Catch-Up với Primary
* `POST /api/admin/delay?ms=3000`: Giả lập độ trễ mạng (mô phỏng node bị lag)
* `POST /api/admin/crash`: Giả lập sập node đột ngột

---

## 🎯 5. Kịch Bản Demo Cho 3 Thành Viên Bảo Vệ

1. **Kịch bản 1 (Student 1):** So sánh độ trễ ghi Đồng bộ (Sync) vs Bất đồng bộ (Async).
2. **Kịch bản 2 (Student 2 - Hảo):** Sập node Backup 2 lúc v5 $\to$ Primary ghi tiếp lên v10 $\to$ Bật lại Backup 2 $\to$ Tự động Catch-Up bắt kịp v10.
3. **Kịch bản 3 (Student 3):** Bật lag 3s cho Backup 1 $\to$ Client ghi key $\to$ Client đọc ngay từ Backup 1 $\to$ Proxy phát hiện vi phạm Read-Your-Writes và tự fallback về Primary để lấy dữ liệu đúng!
4. **Kịch bản 4 (Student 3):** Tắt phụt node đang phục vụ $\to$ Proxy tự động Failover sang node khác mà app không bị văng lỗi.
