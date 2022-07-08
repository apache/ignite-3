package org.apache.ignite.hlc;

import java.time.Clock;

public class SystemTimeProvider implements PhysicalTimeProvider {
    @Override
    public long getPhysicalTime() {
        return Clock.systemUTC().instant().toEpochMilli();
    }
}
