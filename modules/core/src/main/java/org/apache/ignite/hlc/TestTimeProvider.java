package org.apache.ignite.hlc;

import org.apache.ignite.internal.tostring.S;

public class TestTimeProvider implements PhysicalTimeProvider{
    volatile long lastTime;

    volatile boolean doTicks;

    public TestTimeProvider(long initial) {
        this.lastTime = initial;
    }

    @Override
    public synchronized long getPhysicalTime() {
        return lastTime++;
    }

    public synchronized void setLastTime(long lastTime) {
        this.lastTime = lastTime;
    }

    public synchronized void setDoTicks(boolean doTicks) {
        this.doTicks = doTicks;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(TestTimeProvider.class, this);
    }
}
