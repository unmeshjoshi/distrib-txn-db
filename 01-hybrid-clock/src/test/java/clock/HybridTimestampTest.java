package clock;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.jupiter.api.Assertions.*;

class HybridTimestampTest {

    @Test
    public void comparesInAscendingOrder() {
        HybridTimestamp t1 = new HybridTimestamp(1000, 0);
        HybridTimestamp t2 = new HybridTimestamp(1500, 0);
        HybridTimestamp t3 = new HybridTimestamp(2000, 0);

        assertTrue(t2.compareTo(t1) > 0);
        assertTrue(t3.compareTo(t2) > 0);
        assertTrue(t3.compareTo(t1) > 0);
    }
}