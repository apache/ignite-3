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
import org.apache.ignite.internal.cli.core.repl.ConnectionLostEvent;
import org.apache.ignite.internal.cli.core.repl.ConnectionRestoredEvent;
import org.apache.ignite.internal.cli.core.repl.DisconnectEvent;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;

/**
 * Events factory.
 */
public final class Events {

    /**
     * Creates {@code ConnectEvent}.
     *
     * @param sessionInfo session info
     * @return event
     */
    public static Event connect(SessionInfo sessionInfo) {
        return new ConnectEvent(sessionInfo);
    }

    /**
     * Creates {@code DisconnectEvent}.
     *
     * @return event
     */
    public static Event disconnect() {
        return new DisconnectEvent();
    }

    /**
     * Creates {@code ConnectionLostEvent}.
     *
     * @return event
     */
    public static Event connectionLost() {
        return new ConnectionLostEvent();
    }

    /**
     * Creates {@code ConnectionRestoredEvent}.
     *
     * @return event
     */
    public static Event connectionRestored() {
        return new ConnectionRestoredEvent();
    }
}
