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
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.jetbrains.annotations.Nullable;

/**
 * A manager of a server side sql sessions.
 */
public class SessionManager {
    /** Active sessions. */
    private final Map<SessionId, Session> activeSessions = new ConcurrentHashMap<>();

    /**
     * Creates a new session.
     *
     * @param queryProperties Properties to keep within the session.
     * @return A new session.
     */
    public SessionId createSession(
            PropertiesHolder queryProperties
    ) {
        var applied = new AtomicBoolean(false);

        SessionId sessionId;

        do {
            sessionId = nextSessionId();

            activeSessions.computeIfAbsent(sessionId, key -> {
                applied.set(true);

                return new Session(key, queryProperties, () -> activeSessions.remove(key));
            });
        } while (!applied.get());

        return sessionId;
    }

    /**
     * Returns a session for the given id, or {@code null} if this session never exists.
     *
     * @param sessionId An identifier of session of interest.
     * @return A session associated with given id, or {@code null} if this session never exists.
     */
    public @Nullable Session session(SessionId sessionId) {
        return activeSessions.get(sessionId);
    }

    /**
     * Provide list af live session.
     *
     * @return List of active sessions
     */
    public List<SessionInfo> liveSessions() {
        return activeSessions.values().stream().map(SessionInfo::new).collect(Collectors.toList());
    }

    private static SessionId nextSessionId() {
        return new SessionId(UUID.randomUUID());
    }
}
