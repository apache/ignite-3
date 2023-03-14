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

import jakarta.inject.Singleton;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cli.call.unit.ListUnitCall;
import org.apache.ignite.internal.cli.call.unit.UnitStatusRecord;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.repl.AsyncSessionEventListener;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.registry.UnitsRegistry;

/** Implementation of {@link UnitsRegistry}. */
@Singleton
public class UnitsRegistryImpl implements UnitsRegistry, AsyncSessionEventListener {

    private final AtomicReference<String> lastKnownUrl = new AtomicReference<>(null);

    private final ListUnitCall call;

    private final ConcurrentMap<String, Set<String>> idToVersions = new ConcurrentHashMap<>();

    public UnitsRegistryImpl(ListUnitCall call) {
        this.call = call;
    }

    @Override
    public void onConnect(SessionInfo sessionInfo) {
        CompletableFuture.runAsync(() -> updateState(sessionInfo.nodeUrl()));
    }

    private void updateState(String url) {
        try {
            lastKnownUrl.set(url);
            CallOutput<List<UnitStatusRecord>> output = call.execute(new UrlCallInput(url));
            if (!output.hasError() && !output.isEmpty()) {
                output.body().forEach(record -> idToVersions.put(record.id(), record.versionToConsistentIds().keySet()));
            }
        } catch (Exception ignored) {
            // no-op
        }
    }

    @Override
    public void onDisconnect() {
        idToVersions.clear();
    }

    @Override
    public Set<String> versions(String unitId) {
        return idToVersions.get(unitId);
    }

    @Override
    public Set<String> ids() {
        return idToVersions.keySet();
    }

    @Override
    public void refresh() {
        onDisconnect();
        updateState(lastKnownUrl.get());
    }
}
