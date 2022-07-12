package org.apache.ignite.internal.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MetricsRegistry {
    private final Lock lock = new ReentrantLock();

    /** Version always should be changed on metrics enabled/disabled action. */
    private long ver;
    private final Map<String, MetricsSource> sources = new HashMap<>();

    private final Map<String, MetricsSet> sets = new TreeMap<>();

    public void registerSource(MetricsSource src) {
        lock.lock();

        try {
            MetricsSource old = sources.putIfAbsent(src.name(), src);

            if (old != null)
                throw new IllegalStateException("Metrics source with given name is already exists: " + src.name());
        } finally {
            lock.unlock();
        }
    }

    public void unregisterSource(MetricsSource src) {
        lock.lock();

        try {
            disable(src.name());

            sources.remove(src.name());
        } finally {
            lock.unlock();
        }
    }


    public void enable(final String srcName) {
        lock.lock();

        try {
            MetricsSource src = sources.get(srcName);

            if (src == null)
                throw new IllegalStateException("Metrics source with given name doesn't exists: " + srcName);

            sets.put(srcName, src.enable());
        } finally {
            lock.unlock();
        }
    }

    public void disable(final String srcName) {
        lock.lock();

        try {
            MetricsSource src = sources.get(srcName);

            src.disable();

            sets.remove(srcName);
        } finally {
            lock.unlock();
        }
    }
}
