package org.apache.ignite.hlc;

public interface HybridClock {
    HybridTimestamp now();

    HybridTimestamp tick(HybridTimestamp requestTime);
}
