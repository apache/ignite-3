/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.session;

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.sql.engine.AsyncCloseable;
import org.apache.ignite.internal.sql.engine.CurrentTimeProvider;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;

/**
 * A session object.
 *
 * <p>This is a server-side sql session which keeps track of associated resources like opened query cursor, or keeps a properties holder
 * with all properties set during session's creation.
 */
public class Session implements AsyncCloseable {
    /** Marker used to mark a session which has been expired. */
    private static final long EXPIRED = 0L;

    private final Set<AsyncCloseable> resources = Collections.synchronizedSet(
            Collections.newSetFromMap(new IdentityHashMap<>())
    );

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicReference<CompletableFuture<Void>> closeFutRef = new AtomicReference<>();

    private final long idleTimeoutMs;
    private final SessionId sessionId;
    private final AtomicLong lastTouched;
    private final PropertiesHolder queryProperties;
    private final CurrentTimeProvider currentTimeProvider;

    /**
     * Constructor.
     *
     * @param sessionId A session identifier.
     * @param currentTimeProvider The time provider used to update the timestamp on every touch of this object.
     * @param idleTimeoutMs Duration in milliseconds after which the session will be considered expired if no action have been
     *                      performed on behalf of this session during this period.
     * @param queryProperties The properties to keep within.
     */
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

    /** Returns the properties this session associated with. */
    public PropertiesHolder queryProperties() {
        return queryProperties;
    }

    /** Returns the identifier of this session. */
    public SessionId sessionId() {
        return sessionId;
    }

    /** Checks whether the given session has expired or not. */
    public boolean expired() {
        var last = lastTouched.get();

        if (last == EXPIRED) {
            return true;
        }

        return currentTimeProvider.now() - last > idleTimeoutMs
                && (lastTouched.compareAndSet(last, EXPIRED) || lastTouched.get() == EXPIRED);
    }

    /**
     * Updates the timestamp that is used to determine whether the session has expired or not.
     *
     * <p>Note: don't forget to touch the session every time you going to use it, otherwise it might expire in the middle of the operation.
     *
     * @return A {@code true} if this session has been updated, otherwise returns {@code false} means the session has expired.
     */
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
        if (!lock.readLock().tryLock() || expired()) {
            throw new IllegalStateException(format("Attempt to register resource to an expired session [{}]", sessionId));
        }

        try {
            resources.add(resource);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void unregisterResource(AsyncCloseable resource) {
        resources.remove(resource);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        if (closeFutRef.compareAndSet(null, new CompletableFuture<>())) {
            lock.writeLock().lock();

            lastTouched.set(EXPIRED);

            var futs = new CompletableFuture[resources.size()];

            var idx = 0;
            for (var resource : resources) {
                futs[idx++] = resource.closeAsync();
            }

            resources.clear();

            CompletableFuture.allOf(futs).thenRun(() -> closeFutRef.get().complete(null));
        }

        return closeFutRef.get().thenRun(() -> {});
    }
}
