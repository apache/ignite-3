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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * Thrown when session has expired or no longer exists.
 */
public class SessionNotFoundException extends IgniteInternalException {

    private static final long serialVersionUID = -6297499977667006250L;

    /** Constructor. */
    public SessionNotFoundException(SessionId sessionId) {
        super(INTERNAL_ERR, format("Session not found [{}]", sessionId));
    }
}
