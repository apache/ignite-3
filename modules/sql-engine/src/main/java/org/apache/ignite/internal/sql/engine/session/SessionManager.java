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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.CurrentTimeProvider;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jetbrains.annotations.Nullable;

/**
 * A manager of a server side sql sessions.
 */
public class SessionManager {
    private final CurrentTimeProvider timeProvider;

    /** Active sessions with expiration. */
    private Cache<SessionId, Session> activeSessions = Caffeine.newBuilder()
            .weakKeys()
            .weakValues()
            .expireAfter(new Expiry<SessionId, Session>() {
                @Override
                public long expireAfterCreate(SessionId key, Session value, long currentTime) {
                    return TimeUnit.MILLISECONDS.toNanos(value.getIdleTimeoutMs());
                }

                @Override
                public long expireAfterUpdate(SessionId key, Session value, long currentTime, @NonNegative long currentDuration) {
                    return currentDuration;
                }

                @Override
                public long expireAfterRead(SessionId key, Session value, long currentTime, @NonNegative long currentDuration) {
                    return TimeUnit.MILLISECONDS.toNanos(value.getIdleTimeoutMs());
                }
            })
            .build();

    /**
     * Constructor.
     *
     * @param timeProvider A time provider to use for session management.
     */
    public SessionManager(CurrentTimeProvider timeProvider) {
        this.timeProvider = timeProvider;
    }

    /**
     * Creates a new session.
     *
     * @param idleTimeoutMs Duration in milliseconds after which the session will be considered expired if no action have been
     *                     performed on behalf of this session during this period.
     * @param queryProperties Properties to keep within the session.
     * @return A new session.
     */
    public SessionId createSession(
            long idleTimeoutMs,
            PropertiesHolder queryProperties
    ) {
        SessionId sessionId;

        do {
            sessionId = nextSessionId();

            Session ses0 = new Session(sessionId, timeProvider, idleTimeoutMs, queryProperties);

            if (activeSessions.get(sessionId, key -> ses0).equals(ses0)) {
                return sessionId;
            }
        } while (true);
    }

    /**
     * Returns a session for the given id, or {@code null} if this session have already expired or never exists.
     *
     * @param sessionId An identifier of session of interest.
     * @return A session associated with given id, or {@code null} if this session have already expired or never exists.
     */
    public @Nullable Session session(SessionId sessionId) {
        var session = activeSessions.getIfPresent(sessionId);

        if (session != null && !session.touch()) {
            session = null;
        }

        return session;
    }

    private SessionId nextSessionId() {
        return new SessionId(UUID.randomUUID());
    }
}
