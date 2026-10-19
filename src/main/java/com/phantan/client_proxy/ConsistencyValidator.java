package com.phantan.client_proxy;

/**
 * [MODULE BẠN 3 PHỤ TRÁCH]
 * ConsistencyValidator - Kiểm tra các bảo chứng nhất quán hướng Client (Client-Centric Consistency).
 *
 * Lý thuyết phân tán (Slide Lect06 - Module 2 & Tanenbaum Sec 7.2):
 * 1. Read-Your-Writes Consistency:
 *    - Một Client sau khi thực hiện thao tác cập nhật (Write) thì mọi thao tác đọc (Read)
 *      tiếp theo của chính Client đó trên cùng một dữ liệu phải thấy được hiệu ứng của lần ghi đó.
 *    - Điều kiện: ReadVersion >= LastWriteVersion
 * 2. Monotonic-Read Consistency:
 *    - Nếu một Client đã đọc được giá trị x ở phiên bản v tại thời điểm t, thì các lần đọc sau
 *      của Client đó không bao giờ được nhìn thấy phiên bản cũ hơn v (không bị giật lùi về quá khứ).
 *    - Điều kiện: ReadVersion >= LastReadVersion
 */
public class ConsistencyValidator {

    public static boolean checkReadYourWrites(String key, long readVersion, long lastWriteVersion) {
        if (lastWriteVersion <= 0) return true; // Chưa từng ghi key này trong phiên
        return readVersion >= lastWriteVersion;
    }

    public static boolean checkMonotonicReads(String key, long readVersion, long lastReadVersion) {
        if (lastReadVersion <= 0) return true; // Chưa từng đọc key này trong phiên
        return readVersion >= lastReadVersion;
    }
}
