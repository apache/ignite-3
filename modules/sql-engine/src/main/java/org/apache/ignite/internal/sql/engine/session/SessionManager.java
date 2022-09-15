/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.CurrentTimeProvider;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.util.worker.IgniteWorker;
import org.jetbrains.annotations.Nullable;

/**
 * A manager of a server side sql sessions.
 */
public class SessionManager implements LifecycleAware {
    private static final IgniteLogger LOG = Loggers.forClass(SessionManager.class);

    /** Active sessions. */
    private final Map<SessionId, Session> activeSessions = new ConcurrentHashMap<>();

    private final CurrentTimeProvider timeProvider;

    /** Session expiration worker. */
    private final IgniteWorker expirationWorker;

    private final AtomicBoolean startedFlag = new AtomicBoolean(false);

    /**
     * Constructor.
     *
     * @param igniteInstanceName The name of the current node.
     * @param expirationCheckPeriod Time period in milliseconds to check sessions expiration.
     * @param timeProvider A time provider to use for session management.
     */
    public SessionManager(String igniteInstanceName, long expirationCheckPeriod, CurrentTimeProvider timeProvider) {
        this.timeProvider = timeProvider;

        expirationWorker = new IgniteWorker(LOG, igniteInstanceName, "session_cleanup-thread", null) {
            @Override
            protected void body() throws InterruptedException {
                while (!isCancelled()) {
                    blockingSectionBegin();
                    try {
                        Thread.sleep(expirationCheckPeriod);
                    } finally {
                        blockingSectionEnd();
                    }

                    activeSessions.values().stream().filter(Session::expired).forEach((s) -> destroySession(s));

                    LOG.debug("Expired SQL sessions has been cleaned up. Active sessions [count={}]", activeSessions.size());
                }
            }
        };
    }

    /**
     * Creates a new session.
     *
     * @param idleTimeoutMs Duration in milliseconds after which the session will be considered expired if no action have been performed on
     *                      behalf of this session during this period.
     * @param queryProperties Properties to keep within the session.
     * @return A new session.
     */
    public SessionId createSession(
            long idleTimeoutMs,
            PropertiesHolder queryProperties
    ) {
        var applied = new AtomicBoolean(false);

        SessionId sessionId;

        do {
            sessionId = nextSessionId();

            activeSessions.computeIfAbsent(sessionId, key -> {
                applied.set(true);

                return new Session(key, timeProvider, idleTimeoutMs, queryProperties);
            });
        } while (!applied.get());

        return sessionId;
    }

    /**
     * Returns a session for the given id, or {@code null} if this session have already expired or never exists.
     *
     * @param sessionId An identifier of session of interest.
     * @return A session associated with given id, or {@code null} if this session have already expired or never exists.
     */
    public @Nullable Session session(SessionId sessionId) {
        var session = activeSessions.get(sessionId);

        if (session != null && !session.touch()) {
            destroySession(session);
            session = null;
        }

        return session;
    }

    /**
     * Provide list af live session.
     *
     * @return List of active sessions
     */
    public List<SessionInfo> liveSessions() {
        return activeSessions.values().stream().filter((s) -> !s.expired()).map(SessionInfo::new).collect(Collectors.toList());
    }

    /**
     * Destroy a given session.
     *
     * @param session Session which should be destroyed
     */
    private void destroySession(Session session) {
        session.closeAsync();
        activeSessions.remove(session.sessionId());
    }

    private SessionId nextSessionId() {
        return new SessionId(UUID.randomUUID());
    }

    /**
     * Initialize the service by starting session expiration thread.
     */
    @Override
    public void start() {
        if (startedFlag.compareAndSet(false, true)) {
            IgniteThread expirationThread = new IgniteThread(expirationWorker);

            expirationThread.setDaemon(true);
            expirationThread.start();
        }
    }

    /** Stop the service by stopping session expiration thread. */
    @Override
    public void stop() {
        expirationWorker.cancel();
    }
}
