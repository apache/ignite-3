package org.apache.ignite.internal.sql.engine.session;

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.util.IgniteSpinReadWriteLock;

public class Session implements AsyncCloseable {
    private static final long EXPIRED = 0L;

    private final SessionId sessionId;

    private final CurrentTimeProvider currentTimeProvider;

    private final AtomicLong lastTouched;

    private final Set<AsyncCloseable> resources = Collections.synchronizedSet(
            Collections.newSetFromMap(new IdentityHashMap<>())
    );

    private final IgniteSpinReadWriteLock lock = new IgniteSpinReadWriteLock();

    private final long idleTimeoutMs;

    private final PropertiesHolder queryProperties;

    public Session(
            SessionId sessionId,
            CurrentTimeProvider currentTimeProvider,
            long idleTimeoutMs,
            PropertiesHolder queryProperties
    ) {
        this.sessionId = sessionId;
        this.currentTimeProvider = currentTimeProvider;
        this.idleTimeoutMs = idleTimeoutMs;
        this.queryProperties = queryProperties;

        lastTouched = new AtomicLong(currentTimeProvider.now());
    }

    public PropertiesHolder queryProperties() {
        return queryProperties;
    }

    public SessionId sessionId() {
        return sessionId;
    }

    public boolean expired() {
        var last = lastTouched.get();

        if (last == EXPIRED)
            return true;

        return currentTimeProvider.now() - last > idleTimeoutMs && lastTouched.compareAndSet(last, EXPIRED);
    }

    public boolean touch() {
        long time;
        do {
            time = lastTouched.get();

            if (time == EXPIRED) {
                return false;
            }
        } while (!lastTouched.compareAndSet(time, currentTimeProvider.now()));

        return true;
    }

    /**
     * Registers a resource within current session to release in case this session will be closed.
     *
     * <p>This method will throw an {@link IllegalStateException} if someone try to register resource to an
     * already closed/expired session.
     *
     * @param resource Resource to be registered within session.
     */
    public void registerResource(AsyncCloseable resource) {
        if (!lock.tryReadLock() || expired()) {
            throw new IllegalStateException(format("Attempt to register resource to an expired session [{}]", sessionId));
        }

        try {
            resources.add(resource);
        } finally {
            lock.readUnlock();
        }
    }

    public void unregisterResource(AsyncCloseable resource) {
        resources.remove(resource);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        lock.writeLock();

        lastTouched.set(EXPIRED);

        var futs = new CompletableFuture[resources.size()];

        var idx = 0;
        for (var resource : resources) {
            futs[idx++] = resource.closeAsync();
        }

        resources.clear();

        return CompletableFuture.allOf(futs);
    }

    @FunctionalInterface
    interface CurrentTimeProvider {
        long now();
    }
}
