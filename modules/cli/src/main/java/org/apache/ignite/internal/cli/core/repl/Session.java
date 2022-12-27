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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;

/**
 * Connection session that in fact is holder for state: connected or disconnected. Also has session info if the state is connected.
 */
@Singleton
public class Session {

    private static final IgniteLogger log = CliLoggers.forClass(Session.class);

    private final AtomicReference<SessionInfo> info = new AtomicReference<>();

    private final List<AsyncSessionEventListener> listeners;

    public Session(List<AsyncSessionEventListener> listeners) {
        this.listeners = listeners;
    }

    /** Creates session info with provided nodeUrl, nodeName, jdbcUrl. */
    public void connect(SessionInfo newInfo) {
        if (info.compareAndSet(null, newInfo)) {
            listeners.forEach(it -> {
                try {
                    it.onConnect(newInfo);
                } catch (Exception e) {
                    log.warn("Got an exception: ", e);
                }
            });
        } else {
            throw new IllegalStateException();
        }
    }

    /** Clears session info and sets false to connectedToNode. */
    public void disconnect() {
        SessionInfo wasConnected = info.getAndSet(null);
        if (wasConnected != null) {
            listeners.forEach(it -> {
                try {
                    it.onDisconnect();
                } catch (Exception e) {
                    log.warn("Got an exception: ", e);
                }
            });
        }
    }

    /** Returns {@link SessionInfo}. */
    public SessionInfo info() {
        return info.get();
    }
}
