package com.phantan.client_proxy;

import com.phantan.common.KeyValueRecord;

import java.util.Arrays;
import java.util.List;

/**
 * [MODULE BẠN 3 PHỤ TRÁCH]
 * ProxyDemoApp - Ứng dụng Console chạy kịch bản kiểm thử cho Bạn 3:
 * 1. Demo bảo chứng Read-Your-Writes
 * 2. Demo bảo chứng Monotonic Reads
 * 3. Demo tính năng tự động Failover khi sập replica
 */
public class ProxyDemoApp {
    public static void main(String[] args) {
        String primary = "http://localhost:8001";
        List<String> backups = Arrays.asList("http://localhost:8002", "http://localhost:8003");

        SmartClientProxy proxy = new SmartClientProxy(primary, backups);

        System.out.println("======================================================================");
        System.out.println("  [SMART CLIENT PROXY] DEMO BẢO CHỨNG NHẤT QUÁN VÀ FAILOVER");
        System.out.println("======================================================================");

        try {
            // 1. Thao tác ghi qua Proxy
            System.out.println("\n>>> [1] Client thuc hien PUT('account:101', '500_USD')");
            proxy.put("account:101", "500_USD");

            // 2. Thao tác đọc lại ngay lập tức (Read-Your-Writes)
            System.out.println("\n>>> [2] Client thuc hien GET('account:101') ngay sau do:");
            KeyValueRecord rec = proxy.get("account:101");
            if (rec != null) {
                System.out.printf("    => Ket qua doc ra: key='%s', val='%s', v=%d%n",
                        rec.getKey(), rec.getValue(), rec.getVersion());
            }

        } catch (Exception e) {
            System.err.println("Loi: " + e.getMessage());
        }
    }
}
