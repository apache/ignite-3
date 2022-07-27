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

import static org.apache.ignite.lang.ErrorGroups.Sql.SESSION_NOT_FOUND_ERR;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import org.apache.ignite.sql.SqlException;

/**
 * Exception is thrown when someone tries to perform action on behalf of a session that has already expired or never exists.
 */
public class SessionNotFoundException extends SqlException {
    private final SessionId sessionId;

    /**
     * Constructor.
     *
     * @param sessionId A session id.
     */
    public SessionNotFoundException(SessionId sessionId) {
        super(SESSION_NOT_FOUND_ERR, format("Session not found [{}]", sessionId));

        this.sessionId = sessionId;
    }

    /**
     * Returns a sessionId of session which was not found.
     *
     * @return A session id.
     */
    public SessionId sessionId() {
        return sessionId;
    }
}
