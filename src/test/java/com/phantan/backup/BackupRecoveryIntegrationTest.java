package com.phantan.backup;

import com.phantan.common.KeyValueRecord;
import com.phantan.mock.MockPrimaryServer;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kịch bản kiểm thử tích hợp tự động cho Module của BẠN 2 (Hảo):
 * Kiểm chứng:
 * 1. Khả năng lưu trữ bền vững (Local Persistence)
 * 2. Nhận luồng replication từ Primary
 * 3. Sập node (Crash simulation) khi đang ở v5
 * 4. Primary tiếp tục ghi dữ liệu lên v10 trong lúc Backup chết
 * 5. Khởi động lại Backup -> Tự động kích hoạt Catch-Up Recovery bắt kịp v10
 * 6. Kiểm tra toàn vẹn dữ liệu trên đĩa sau khi phục hồi
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BackupRecoveryIntegrationTest {

    private static final int PRIMARY_PORT = 9001;
    private static final int BACKUP_PORT = 9002;
    private static final String DATA_DIR = "./target/test_backup_data";

    private static MockPrimaryServer primaryServer;

    @BeforeAll
    public static void setUpAll() throws Exception {
        // Xóa sạch thư mục test trước đó
        deleteDirectory(new File(DATA_DIR));

        // Khởi động Mock Primary
        primaryServer = new MockPrimaryServer(PRIMARY_PORT);
        primaryServer.setReplicaUrls(java.util.List.of("http://localhost:" + BACKUP_PORT));
        primaryServer.start();
    }

    @AfterAll
    public static void tearDownAll() {
        if (primaryServer != null) {
            primaryServer.stop();
        }
        deleteDirectory(new File(DATA_DIR));
    }

    @Test
    @Order(1)
    @DisplayName("Kịch bản Demo Trọng Tâm: Sập Node lúc v5 -> Primary lên v10 -> Backup hồi sinh và Catch-up hoàn hảo")
    public void testCrashAndCatchUpRecovery() throws Exception {
        System.out.println("\n>>> [TEST BƯỚC 1]: Khởi động Backup Replica lần đầu (v0)");
        BackupServer backupServer = new BackupServer("backup-test-1", BACKUP_PORT, DATA_DIR, "http://localhost:" + PRIMARY_PORT);
        backupServer.start();

        Thread.sleep(1500); // Đợi server ổn định
        assertEquals(0, backupServer.getLocalStorage().getCurrentVersion(), "Ban đầu version phải bằng 0");

        System.out.println("\n>>> [TEST BƯỚC 2]: Primary ghi 5 keys đầu tiên (v1 -> v5). Backup nhận replication.");
        for (int i = 1; i <= 5; i++) {
            primaryServer.write("key_" + i, "value_" + i, true);
        }

        // Chờ replication đẩy sang Backup
        Thread.sleep(1000);

        assertEquals(5, backupServer.getLocalStorage().getCurrentVersion(), "Backup phải đạt v5 sau khi nhận replication");
        assertEquals("value_3", backupServer.getLocalStorage().get("key_3").getValue());
        System.out.println(">>> [TEST BƯỚC 2 THÀNH CÔNG]: Backup đã đồng bộ đạt v5.");

        System.out.println("\n>>> [TEST BƯỚC 3]: GIẢ LẬP SẬP NODE! (Tắt Backup Replica đột ngột lúc v5)");
        backupServer.stop();
        System.out.println(">>> Backup Node đã OFFLINE.");

        System.out.println("\n>>> [TEST BƯỚC 4]: Trong lúc Backup sập, Primary tiếp tục nhận ghi 5 keys mới (v6 -> v10)");
        for (int i = 6; i <= 10; i++) {
            primaryServer.write("key_" + i, "value_" + i, true);
        }

        System.out.println(">>> Primary hiện đã ở v10. Backup vẫn đang chết ở v5.");

        System.out.println("\n>>> [TEST BƯỚC 5]: HỒI SINH BACKUP NODE! (Bật lại server)");
        BackupServer resurrectedBackup = new BackupServer("backup-test-1", BACKUP_PORT, DATA_DIR, "http://localhost:" + PRIMARY_PORT);

        // Khẳng định đĩa cứng vẫn nhớ v5 (Local Persistence verification)
        assertEquals(5, resurrectedBackup.getLocalStorage().getCurrentVersion(),
                "Khi sống lại, LocalStorage phải nạp lại đúng v5 từ đĩa cứng (không bị mất dữ liệu)");

        resurrectedBackup.start();

        // Chờ Auto-CatchUp kích hoạt và hoàn tất
        Thread.sleep(2500);

        System.out.println("\n>>> [TEST BƯỚC 6]: Kiểm tra Backup đã Catch-up lên v10 chưa");
        assertEquals(10, resurrectedBackup.getLocalStorage().getCurrentVersion(),
                "Backup sau khi Catch-Up phải đạt đúng v10");

        // Kiểm tra tất cả 10 keys đều tồn tại đầy đủ trên Backup
        for (int i = 1; i <= 10; i++) {
            KeyValueRecord record = resurrectedBackup.getLocalStorage().get("key_" + i);
            assertNotNull(record, "Key key_" + i + " phải tồn tại");
            assertEquals("value_" + i, record.getValue(), "Value của key_" + i + " phải khớp");
            assertEquals(i, record.getVersion(), "Version của key_" + i + " phải bằng " + i);
        }

        System.out.println(">>> [XÁC NHẬN]: Toàn bộ 10 keys từ v1 đến v10 đã có mặt đầy đủ trên đĩa và RAM của Backup!");

        // Dọn dẹp
        resurrectedBackup.stop();
        System.out.println("\n>>> [KẾT QUẢ]: KỊCH BẢN CATCH-UP RECOVERY CỦA BẠN 2 ĐẠT ĐIỂM TUYỆT ĐỐI (PASSED 100%)!\n");
    }

    private static void deleteDirectory(File dir) {
        if (dir.exists()) {
            try {
                Files.walk(dir.toPath())
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (Exception ignored) {}
        }
    }
}
