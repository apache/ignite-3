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
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Connection session that in fact is holder for state: connected or disconnected. Also has a nodeUrl if the state is connected.
 */
@Singleton
public class Session {

    private final IgniteLogger log = Loggers.forClass(getClass());

    private SessionDetails sessionDetails;

    private boolean connectedToNode;

    private final List<SessionEventListener> listeners;

    public Session(List<SessionEventListener> listeners) {
        this.listeners = listeners;
    }

    public synchronized void connect(String nodeUrl, String nodeName, String jdbcUrl) {
        this.sessionDetails = new SessionDetails(nodeUrl, nodeName, jdbcUrl);
        this.connectedToNode = true;
        listeners.forEach(it -> {
            try {
                it.onConnect(this);
            } catch (Exception e) {
                log.warn("Got an exception: ", e);
            }
        });
    }

    public synchronized void disconnect() {
        this.sessionDetails = new SessionDetails();
        this.connectedToNode = false;
        listeners.forEach(it -> {
            try {
                it.onConnect(this);
            } catch (Exception e) {
                log.warn("Got an exception: ", e);
            }
        });
    }

    public boolean isConnectedToNode() {
        return connectedToNode;
    }

    public SessionDetails sessionDetails() {
        return this.sessionDetails;
    };
}
