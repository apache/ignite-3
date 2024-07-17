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

package org.apache.ignite.internal.cli.event;

import org.apache.ignite.internal.cli.core.repl.ConnectEvent;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;

/**
 * Listener of any connection related events.
 */
public interface ConnectionEventListener extends EventListener {

    @Override
    default void onEvent(Event event) {
        switch (event.eventType()) {
            case CONNECT:
                ConnectEvent connectEvent = (ConnectEvent) event;
                onConnect(connectEvent.sessionInfo());
                break;
            case DISCONNECT:
                onDisconnect();
                break;
            case CONNECTION_LOST:
                onConnectionLost();
                break;
            case CONNECTION_RESTORED:
                onConnectionRestored();
                break;
            default:
                break;
        }
    }

    /** Implementation must be async. */
    default void onConnect(SessionInfo sessionInfo) {
    }

    /** Implementation must be async. */
    default void onDisconnect() {
    }

    /** Implementation must be async. */
    default void onConnectionLost() {
    }

    /** Implementation must be async. */
    default void onConnectionRestored() {
    }

}
