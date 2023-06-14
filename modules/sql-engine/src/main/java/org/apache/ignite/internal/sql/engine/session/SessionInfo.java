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
        this.queryProperties = session.properties();
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
