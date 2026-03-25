package kv;

import clock.HybridTimestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Optional;

import static kv.MemcomparableCodec.encodeString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
class RocksDbMvccStoreTest {

    @Test
    public void testPutAndGet(@TempDir java.nio.file.Path tempDir) {
        try (RocksDbMvccStore store = new RocksDbMvccStore(tempDir)) {
            MVCCKey key = newKey("Author", 1000L);
            store.put(key, encodeString("Martin"));

            Optional<byte[]> author = store.getLatest(encodeString("Author"));
            assertOptionalBytesEquals(encodeString("Martin"), author);
        }
    }

    @Test
    public void needsMemcompatibleEncoding(@TempDir java.nio.file.Path tempDir) {
        try (RocksDbMvccStore store = new RocksDbMvccStore(tempDir)) {
            MVCCKey key = newKey("Author", 1000l);
            MVCCKey keyV2 = newKey("Author", 2000l);

            store.put(key, encodeString("Martin"));
            store.put(keyV2, encodeString("Unmesh"));

            MVCCKey key2 = newKey("Title", 1000l);
            store.put(key2, encodeString("The Art of Computer Programming"));

            Optional<byte[]> author = store.getLatest(encodeString("Author"));
            assertOptionalBytesEquals(encodeString("Unmesh"), author);

            Optional<byte[]> title = store.getLatest(encodeString("Title"));
            assertOptionalBytesEquals(
                    encodeString("The Art of Computer Programming"), title);
        }
    }

    private static MVCCKey newKey(String key, long wallClockTime) {
        return new MVCCKey(encodeString(key),
                new HybridTimestamp(
                        wallClockTime, 0));
    }

    private static void assertOptionalBytesEquals(byte[] expected, Optional<byte[]> actual) {
        assertTrue(actual.isPresent());
        assertArrayEquals(expected, actual.get());
    }

}
