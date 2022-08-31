package org.apache.ignite.internal.sql.engine.session;

import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;

/**
 * Session descriptor.
 */
public class SessionInfo {

    /** A session identifier. */
    private final SessionId sessionId;

    /** The properties this session associated with. */
    private final PropertiesHolder queryProperties;

    /**
     * Duration in milliseconds after which the session will be considered expired if no action have been performed on behalf of this
     * session during this period.
     */
    private final long idleTimeoutMs;

    /**
     * Constructor.
     *
     * @param session Session object,
     */
    public SessionInfo(Session session) {
        this.sessionId = session.sessionId();
        this.queryProperties = session.queryProperties();
        this.idleTimeoutMs = getIdleTimeoutMs();
    }

    /** Returns the identifier of this session. */
    public SessionId getSessionId() {
        return sessionId;
    }

    /** Returns the properties this session associated with. */
    public PropertiesHolder getQueryProperties() {
        return queryProperties;
    }

    /** Returns the duration in millis after which the session will be considered expired if no one touched it in the middle. */
    public long getIdleTimeoutMs() {
        return idleTimeoutMs;
    }
}
