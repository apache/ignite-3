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

package org.apache.ignite.internal.cli.core.repl.registry.impl;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.event.ConnectionEventListener;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for registry that uses lazy object reference and can be refreshed.
 *
 * @param <T> Result type.
 */
public abstract class RegistryImplBase<T> implements ConnectionEventListener {
    private final AtomicReference<String> lastKnownUrl = new AtomicReference<>(null);

    @Nullable
    private LazyObjectRef<T> ref;

    @Override
    public void onConnect(SessionInfo sessionInfo) {
        lastKnownUrl.set(sessionInfo.nodeUrl());
        updateState();
    }

    private void updateState() {
        String lastKnownUrl = this.lastKnownUrl.get();
        if (lastKnownUrl != null) {
            ref = new LazyObjectRef<>(() -> doGetState(lastKnownUrl));
        }
    }

    @Nullable
    protected abstract T doGetState(String url);

    @Override
    public void onDisconnect() {
        ref = null;
    }

    @Nullable
    protected T getResult() {
        return ref != null ? ref.get() : null;
    }

    public void refresh() {
        updateState();
    }
}
