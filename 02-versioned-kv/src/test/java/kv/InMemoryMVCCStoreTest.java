package kv;

import clock.HybridTimestamp;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static kv.TestUtils.ts;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryMVCCStoreTest {

    private byte[] encode(String v) { return OrderPreservingCodec.encodeString(v); }
    private String decode(byte[] v) { return OrderPreservingCodec.decodeString(v); }

    @Test
    void testStoreAndFetchLatest() {
        MVCCStore store = new InMemoryMVCCStore();

        var t1 = ts(1000);
        var t2 = ts(1500);
        var t3 = ts(2000);

        String accountID = "account1";
        store.put(new MVCCKey(encode(accountID), t1), encode("{balance:100}"));
        store.put(new MVCCKey(encode(accountID), t2), encode("{balance:500}"));
        store.put(new MVCCKey(encode(accountID), t3), encode("{balance:200}"));

        Optional<byte[]> latest = store.getLatest(encode(accountID));
        assertTrue(latest.isPresent());
        assertEquals("{balance:200}", decode(latest.get()));
    }

    @Test
    void testFetchAsOfTime() {
        MVCCStore store = new InMemoryMVCCStore();

        var t1 = ts(1000);
        var t2 = ts(1500);
        var t3 = ts(2000);

        String accountId = "account1";
        store.put(new MVCCKey(encode(accountId), t1), encode("{balance:100}"));
        store.put(new MVCCKey(encode(accountId), t2), encode("{balance:500}"));
        store.put(new MVCCKey(encode(accountId), t3), encode("{balance:200}"));

        Optional<byte[]> asOfT2 = store.getAsOf(new MVCCKey(encode(accountId), t2));
        assertTrue(asOfT2.isPresent());
        assertEquals("{balance:500}", decode(asOfT2.get()));

        Optional<byte[]> asOfMid = store.getAsOf(new MVCCKey(encode(accountId), ts(1200)));
        assertTrue(asOfMid.isPresent());
        assertEquals("{balance:100}", decode(asOfMid.get()));

        Optional<byte[]> beforeT1 = store.getAsOf(new MVCCKey(encode(accountId), ts(500)));
        assertTrue(beforeT1.isEmpty());
    }

    @Test
    void testCrossKeyBoundary() {
        MVCCStore store = new InMemoryMVCCStore();

        var t1 = ts(1000);
        var t2 = ts(1500);

        store.put(new MVCCKey(encode("key1"), t1), encode("valueA1"));
        store.put(new MVCCKey(encode("key2"), t2), encode("valueB2"));

        Optional<byte[]> noKey1 = store.getAsOf(new MVCCKey(encode("key1"), ts(500)));
        assertTrue(noKey1.isEmpty());
    }

    @Test
    void testGetVersionsUpTo() {
        MVCCStore store = new InMemoryMVCCStore();
        var t1 = ts(1000);
        var t2 = ts(1500);
        var t3 = ts(2000);

        store.put(new MVCCKey(encode("k1"), t1), encode("v1"));
        store.put(new MVCCKey(encode("k1"), t2), encode("v2"));
        store.put(new MVCCKey(encode("k1"), t3), encode("v3"));

        Map<HybridTimestamp, byte[]> values = store.getVersionsUpTo(encode("k1"), t2);
        assertEquals(2, values.size());
        Iterator<Map.Entry<HybridTimestamp, byte[]>> iterator = values.entrySet().iterator();
        Map.Entry<HybridTimestamp, byte[]> first = iterator.next();
        assertEquals(t1, first.getKey());
        assertArrayEquals(encode("v1"), first.getValue());
        Map.Entry<HybridTimestamp, byte[]> second = iterator.next();
        assertEquals(t2, second.getKey());
        assertArrayEquals(encode("v2"), second.getValue());
    }

    @Test
    void testScanPrefixAsOfReturnsVisibleVersionPerLogicalKey() {
        MVCCStore store = new InMemoryMVCCStore();
        var t1 = ts(1000);
        var t2 = ts(2000);
        var t3 = ts(3000);

        store.put(new MVCCKey(encode("customers_customer_1_name"), t1), encode("Alice"));
        store.put(new MVCCKey(encode("customers_customer_1_address"), t1), encode("Old Address"));
        store.put(new MVCCKey(encode("customers_customer_1_address"), t2), encode("Mid Address"));
        store.put(new MVCCKey(encode("customers_customer_1_address"), t3), encode("New Address"));
        store.put(new MVCCKey(encode("customers_customer_2_name"), t1), encode("Bob"));

        Map<byte[], byte[]> values = store.scanPrefixAsOf(encode("customers_customer_1_"), t2);
        assertEquals(2, values.size());
        assertEquals("Alice", decode(values.get(encode("customers_customer_1_name"))));
        assertEquals("Mid Address", decode(values.get(encode("customers_customer_1_address"))));

    }
}
