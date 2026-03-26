package kv;

import org.junit.jupiter.api.Test;

import static kv.TestUtils.ts;
import static org.junit.jupiter.api.Assertions.*;

class MVCCKeyTest {
    @Test
    public void comparesInAscendingTimestampOrder() {
        MVCCKey v1 = new MVCCKey("author".getBytes(), ts(1000));
        MVCCKey v2 = new MVCCKey("author".getBytes(), ts(1500));
        MVCCKey v3 = new MVCCKey("author".getBytes(), ts(2000));

        assertTrue(v1.compareTo(v2) < 0);
        assertTrue(v2.compareTo(v3) < 0);
        assertTrue(v1.compareTo(v3) < 0);
    }

    @Test
    public void testIsVisibleAt() {
        MVCCKey v1 = new MVCCKey("author".getBytes(), ts(1000));
        MVCCKey v2 = new MVCCKey("author".getBytes(), ts(1500));
        MVCCKey v3 = new MVCCKey("author".getBytes(), ts(2000));

        assertTrue(v1.isVisibleAt(ts(1000)));
        assertTrue(v1.isVisibleAt(ts(1500)));
        assertTrue(v1.isVisibleAt(ts(2000)));

        assertFalse(v1.isVisibleAt(ts(999)));
        assertFalse(v2.isVisibleAt(ts(1000)));
    }

    @Test
    public void testHasPrefix() {
        MVCCKey v1 = new MVCCKey("customers_id1_name".getBytes(), ts(1000));
        assertTrue(v1.startsWith("customers_id1".getBytes()));
    }
}
