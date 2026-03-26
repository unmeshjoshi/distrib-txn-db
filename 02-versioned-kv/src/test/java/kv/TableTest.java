package kv;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;

import static kv.TestUtils.ts;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TableTest {

    private final String dbPath = "/tmp/rocksdb-table-colocation-test";
    private MVCCStore sharedStore;

    @BeforeEach
    void setUp() {
        deleteDirectory(new File(dbPath));
        // A single RocksDB instance serves as the backbone for the entire test
        sharedStore = new RocksDbMvccStore(Path.of(dbPath));
    }

    @AfterEach
    void tearDown() {
        if (sharedStore != null) {
            sharedStore.close();
        }
        deleteDirectory(new File(dbPath));
    }

    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) deleteDirectory(file);
                    else file.delete();
                }
            }
            dir.delete();
        }
    }

    @Test
    void testMultiColumnRowStorage() {
        // Create 2 completely isolated logical tables that write precisely to the SAME RocksDB backend
        Table customersTable = new Table("customers", sharedStore);
        Table ordersTable = new Table("orders", sharedStore);

        var t1 = ts(1000);
        var t2 = ts(2000);

        // Store typical customer attributes across multiple columns for "customer_1"
        customersTable.put("customer_1", "name", t1, "Alice Smith");
        customersTable.put("customer_1", "email", t1, "alice@example.com");
        customersTable.put("customer_1", "address", t1, "123 Main St");

        // Update the email later at T2 simulating a user profile edit
        customersTable.put("customer_1", "email", t2, "alice.smith@newdomain.com");

        // Store a complete representation of an order in the parallel table
        ordersTable.put("order_99", "customerId", t1, "customer_1");
        ordersTable.put("order_99", "total_amount", t1, "$250.00");
        ordersTable.put("order_99", "status", t1, "SHIPPED");

        // Validate customers table fetches (Latest Data)
        assertEquals("Alice Smith", customersTable.get("customer_1", "name").get());
        assertEquals("123 Main St", customersTable.get("customer_1", "address").get());
        
        // Assert the update was successful for T2
        assertEquals("alice.smith@newdomain.com", customersTable.get("customer_1", "email").get());
        
        // Assert MVCC functionality isolated perfectly at T1 for the specific email column
        assertEquals("alice@example.com", customersTable.get("customer_1", "email", t1).get(), 
            "As of T1, email should be original");

        // Validate orders table multiplexing
        assertEquals("customer_1", ordersTable.get("order_99", "customerId").get());
        assertEquals("SHIPPED", ordersTable.get("order_99", "status").get());
        assertEquals("$250.00", ordersTable.get("order_99", "total_amount").get());
    }

    @Test
    void testAtomicRowInsertion() {
        Table customersTable = new Table("customers", sharedStore);
        var t1 = ts(1000);
        var t2 = ts(2000);
        var t3 = ts(3000);

        java.util.Map<String, String> rowV1 = new java.util.HashMap<>();
        rowV1.put("name", "Alice Smith");
        rowV1.put("email", "alice@example.com");
        rowV1.put("address", "123 Main St");

        // Dynamically bulk insert via atomic WriteBatch 
        customersTable.insertRow("customer_1", rowV1, t1);

        java.util.Map<String, String> rowV2 = new java.util.HashMap<>();
        rowV2.put("name", "Alice Smith");
        rowV2.put("email", "alice@example.com");
        rowV2.put("address", "220 Main Street");
        // Dynamically bulk insert via atomic WriteBatch
        customersTable.insertRow("customer_1", rowV2, t2);

        java.util.Map<String, String> rowV3 = new java.util.HashMap<>();
        rowV3.put("name", "Alice Johnson");
        rowV3.put("email", "alice.johnson@example.com");
        rowV3.put("address", "42 Ocean Avenue");
        customersTable.insertRow("customer_1", rowV3, t3);

        // Validate latest row retrieval
        assertEquals("Alice Johnson", customersTable.get("customer_1", "name").get());
        assertEquals("alice.johnson@example.com", customersTable.get("customer_1", "email").get());
        assertEquals("42 Ocean Avenue", customersTable.get("customer_1", "address").get());

        // Validate latest complete row parsing
        java.util.Map<String, String> completeRow = customersTable.getRow("customer_1");
        assertEquals(3, completeRow.size());
        assertEquals("Alice Johnson", completeRow.get("name"));
        assertEquals("alice.johnson@example.com", completeRow.get("email"));
        assertEquals("42 Ocean Avenue", completeRow.get("address"));

        java.util.Map<String, String> historicalRowAtT1 = customersTable.getRow("customer_1", t1);
        assertEquals(3, historicalRowAtT1.size());
        assertEquals("Alice Smith", historicalRowAtT1.get("name"));
        assertEquals("alice@example.com", historicalRowAtT1.get("email"));
        assertEquals("123 Main St", historicalRowAtT1.get("address"));

        java.util.Map<String, String> historicalRowAtT2 = customersTable.getRow("customer_1", t2);
        assertEquals(3, historicalRowAtT2.size());
        assertEquals("Alice Smith", historicalRowAtT2.get("name"));
        assertEquals("alice@example.com", historicalRowAtT2.get("email"));
        assertEquals("220 Main Street", historicalRowAtT2.get("address"));

        java.util.Map<String, String> historicalRowAtT3 = customersTable.getRow("customer_1", t3);
        assertEquals(3, historicalRowAtT3.size());
        assertEquals("Alice Johnson", historicalRowAtT3.get("name"));
        assertEquals("alice.johnson@example.com", historicalRowAtT3.get("email"));
        assertEquals("42 Ocean Avenue", historicalRowAtT3.get("address"));
        
        java.util.Map<String, String> historicalRow = customersTable.getRow("customer_1", ts(500));
        assertEquals(0, historicalRow.size(), "Row shouldn't exist prior to logical bounds assignment!");
    }
}
