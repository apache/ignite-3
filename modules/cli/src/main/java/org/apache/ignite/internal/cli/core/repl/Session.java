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
import org.apache.ignite.internal.cli.core.exception.ConnectionException;
import org.apache.ignite.internal.cli.event.AsyncConnectionEventListener;

/**
 * Connection session that in fact is holder for state: connected or disconnected. Also has session info if the state is connected.
 */
@Singleton
public class Session extends AsyncConnectionEventListener {

    private final AtomicReference<SessionInfo> info = new AtomicReference<>();

    public Session() {
    }

    @Override
    protected void onConnect(SessionInfo sessionInfo) {
        if (!info.compareAndSet(null, sessionInfo)) {
            throw new ConnectionException("Already connected to " + info.get().nodeUrl());
        }
    }

    @Override
    protected void onDisconnect() {
        info.getAndSet(null);
    }

    /** Returns {@link SessionInfo}. */
    public SessionInfo info() {
        return info.get();
    }
}
