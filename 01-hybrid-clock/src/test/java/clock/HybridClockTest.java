package clock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tickloom.util.StubClock;

class HybridClockTest {
    @Test
    void testLogicalTickWhenPhysicalTimeUnchanged() {
        StubClock stubClock = new StubClock(1000);
        HybridClock hybridClock = new HybridClock(stubClock);

        HybridTimestamp t1 = hybridClock.now();
        assertEquals(1000, t1.getWallClockTime());
        assertEquals(1, t1.getTicks());

        HybridTimestamp t2 = hybridClock.now();
        assertEquals(1000, t2.getWallClockTime());
        assertEquals(2, t2.getTicks());
    }

    @Test
    void testUpdateFromFutureReceivedTimestamp() {
        StubClock stubClock = new StubClock(1000);
        HybridClock hybridClock = new HybridClock(stubClock);

        HybridTimestamp received = new HybridTimestamp(1500, 5);
        HybridTimestamp t = hybridClock.tick(received);
        
        assertEquals(1500, t.getWallClockTime());
        // maxTime == requestTime.getWallClockTime(), ticks should be requestTime.getTicks() + 1
        assertEquals(6, t.getTicks());
    }

    @Test
    void testUpdateFromPastReceivedTimestamp() {
        StubClock stubClock = new StubClock(1000);
        HybridClock hybridClock = new HybridClock(stubClock);

        // hybrid clock has 1000, 0
        // after now() it's 1000, 1
        hybridClock.now();

        HybridTimestamp received = new HybridTimestamp(500, 0);
        HybridTimestamp t = hybridClock.tick(received);
        
        // System time is 1000, largest is system time. 
        // Max is system time (1000). So it ticks logical clock since latestTime is also 1000.
        assertEquals(1000, t.getWallClockTime());
        assertEquals(2, t.getTicks());

        // And verify we can advance the clock easily
        stubClock.advance(500);
        HybridTimestamp advanced = hybridClock.now();
        assertEquals(1500, advanced.getWallClockTime());
        assertEquals(0, advanced.getTicks());
    }
}
