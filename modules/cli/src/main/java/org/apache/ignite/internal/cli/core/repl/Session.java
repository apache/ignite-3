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

package org.apache.ignite.internal.cli.core.repl;

import jakarta.inject.Singleton;
import java.util.List;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;

/**
 * Connection session that in fact is holder for state: connected or disconnected. Also has a nodeUrl if the state is connected.
 */
@Singleton
public class Session {

    private static final IgniteLogger log = CliLoggers.forClass(Session.class);

    private SessionContext sessionContext;

    private boolean connectedToNode;

    private final List<SessionEventListener> listeners;

    public Session(List<SessionEventListener> listeners) {
        this.listeners = listeners;
    }

    /** Creates session details with provided nodeUrl, nodeName, jdbcUrl. */
    public synchronized void connect(SessionContext context) {
        this.sessionContext = context;
        this.connectedToNode = true;
        listeners.forEach(it -> {
            try {
                it.onConnect(this);
            } catch (Exception e) {
                log.warn("Got an exception: ", e);
            }
        });
    }

    /** Clears session details and sets false to connectedToNode. */
    public synchronized void disconnect() {
        this.sessionContext = new SessionContext();
        this.connectedToNode = false;
        listeners.forEach(it -> {
            try {
                it.onDisconnect();
            } catch (Exception e) {
                log.warn("Got an exception: ", e);
            }
        });
    }

    public boolean isConnectedToNode() {
        return connectedToNode;
    }

    /** Returns {@link SessionContext}. */
    public SessionContext sessionDetails() {
        return this.sessionContext;
    }
}
