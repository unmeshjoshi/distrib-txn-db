package kv;

import clock.HybridTimestamp;
import org.junit.jupiter.api.Test;

import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

class MVCCKeyTest {
    @Test
    public void comparesInDescendingTimestampOrder() {
        MVCCKey v1 = new MVCCKey("author".getBytes(), new HybridTimestamp(1000, 0));
        MVCCKey v2 = new MVCCKey("author".getBytes(), new HybridTimestamp(1500, 0));
        MVCCKey v3 = new MVCCKey("author".getBytes(), new HybridTimestamp(2000, 0));

        assertTrue(v1.compareTo(v2) < 0);
        assertTrue(v2.compareTo(v3) < 0);
        assertTrue(v1.compareTo(v3) < 0);
    }

}