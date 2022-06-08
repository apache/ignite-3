package org.apache.ignite.internal.sql.engine.session;

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import org.apache.ignite.sql.SqlException;

public class SessionNotFound extends SqlException {
    private final SessionId sessionId;

    public SessionNotFound(SessionId sessionId) {
        super(format("Session not found [{}]", sessionId));

        this.sessionId = sessionId;
    }

    public SessionId sessionId() {
        return sessionId;
    }
}
