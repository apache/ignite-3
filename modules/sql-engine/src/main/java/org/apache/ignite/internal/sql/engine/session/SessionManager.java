package org.apache.ignite.internal.sql.engine.session;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.jetbrains.annotations.Nullable;

public class SessionManager {
    private final Map<SessionId, Session> activeSessions = new ConcurrentHashMap<>();

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

                return new Session(key, System::currentTimeMillis, idleTimeoutMs, queryProperties);
            });
        } while (!applied.get());

        return sessionId;
    }

    public @Nullable Session session(SessionId sessionId) {
        var session = activeSessions.get(sessionId);

        if (session != null && !session.touch()) {
            session = null;
        }

        return session;
    }

    private SessionId nextSessionId() {
        return new SessionId(UUID.randomUUID());
    }
}
