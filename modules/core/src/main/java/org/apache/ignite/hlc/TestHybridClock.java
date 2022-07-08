package org.apache.ignite.hlc;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.tostring.S;

public class TestHybridClock implements HybridClock{
    static int x = 0;
    private final PhysicalTimeProvider physicalTimeProvider;
    /** Latest timestamp. */
    private HybridTimestamp latestTime;

    private List<HybridTimestamp[]> times = new ArrayList<>();
    /**
     * The constructor which initializes the latest time to current time by system clock.
     */
    public TestHybridClock(PhysicalTimeProvider physicalTimeProvider) {
        this.physicalTimeProvider = physicalTimeProvider;

        this.latestTime = new HybridTimestamp(physicalTimeProvider.getPhysicalTime(), 0);
    }

    /**
     * Creates a timestamp for new event.
     *
     * @return The hybrid timestamp.
     */
    public synchronized HybridTimestamp now() {
        long currentTimeMillis = physicalTimeProvider.getPhysicalTime();

        if (latestTime.getPhysical() >= currentTimeMillis) {
            latestTime = latestTime.addTicks(1);
        } else {
            latestTime = new HybridTimestamp(currentTimeMillis, 0);
        }

        System.out.println("Clocl.now() " + this + " hash " + this.hashCode());

        times.add(new HybridTimestamp[]{latestTime, new HybridTimestamp(currentTimeMillis, 0)});

//        x++;

        return latestTime;
    }

    /**
     * Creates a timestamp for a received event.
     *
     * @param requestTime Timestamp from request.
     * @return The hybrid timestamp.
     */
    public synchronized HybridTimestamp tick(HybridTimestamp requestTime) {
        HybridTimestamp now = new HybridTimestamp(physicalTimeProvider.getPhysicalTime(), -1);

        latestTime = HybridTimestamp.max(now, requestTime, latestTime);

        latestTime = latestTime.addTicks(1);

        System.out.println("Clocl.tick() " + this + " hash " + this.hashCode());

        times.add(new HybridTimestamp[]{latestTime, now, requestTime});

//        x++;

        return latestTime;//latestTime.getPhysical() == 2 && latestTime.getLogical() == 1
    }

    public PhysicalTimeProvider getPhysicalTimeProvider() {
        return physicalTimeProvider;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(TestHybridClock.class, this);
    }
}
