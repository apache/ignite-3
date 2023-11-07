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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cli.event.ConnectionEventListener;

/**
 * Connection session that in fact is holder for state: connected or disconnected. Also has session info if the state is connected.
 */
@Singleton
public class Session implements ConnectionEventListener {

    private final AtomicReference<SessionInfo> info = new AtomicReference<>();

    @Override
    public void onConnect(SessionInfo sessionInfo) {
        info.set(sessionInfo);
    }

    @Override
    public void onDisconnect() {
        info.set(null);
    }

    /** Returns {@link SessionInfo}. */
    public SessionInfo info() {
        return info.get();
    }
}
