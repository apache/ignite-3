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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.call.cluster.unit.ClusterListUnitCall;
import org.apache.ignite.internal.cli.call.unit.ListUnitCallInput;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.registry.UnitsRegistry;
import org.apache.ignite.internal.cli.event.ConnectionEventListener;
import org.apache.ignite.rest.client.model.UnitStatus;
import org.apache.ignite.rest.client.model.UnitVersionStatus;
import org.jetbrains.annotations.Nullable;

/** Implementation of {@link UnitsRegistry}. */
@Singleton
public class UnitsRegistryImpl implements UnitsRegistry, ConnectionEventListener {

    private final AtomicReference<String> lastKnownUrl = new AtomicReference<>(null);

    private final ClusterListUnitCall call;

    @Nullable
    private LazyObjectRef<Map<String, Set<String>>> idToVersionsRef;

    public UnitsRegistryImpl(ClusterListUnitCall call) {
        this.call = call;
    }

    @Override
    public void onConnect(SessionInfo sessionInfo) {
        updateState(sessionInfo.nodeUrl());
    }

    private void updateState(String url) {
        lastKnownUrl.set(url);

        idToVersionsRef = new LazyObjectRef<>(() -> {
            ListUnitCallInput input = ListUnitCallInput.builder()
                    .url(url)
                    .build();
            CallOutput<List<UnitStatus>> output = call.execute(input);
            if (!output.hasError() && !output.isEmpty()) {

                return output.body().stream()
                        .collect(Collectors.toMap(
                                UnitStatus::getId,
                                status -> status.getVersionToStatus()
                                        .stream().map(UnitVersionStatus::getVersion).collect(Collectors.toSet()))
                        );
            } else {
                return null;
            }
        });
    }

    @Override
    public void onDisconnect() {
        idToVersionsRef = null;
    }

    @Override
    public Set<String> versions(String unitId) {
        Map<String, Set<String>> idToVersions = idToVersions();
        return idToVersions == null ? Set.of() : idToVersions.get(unitId);
    }

    @Override
    public Set<String> ids() {
        Map<String, Set<String>> idToVersions = idToVersions();
        return idToVersions == null ? Set.of() : idToVersions.keySet();
    }

    @Nullable
    private Map<String, Set<String>> idToVersions() {
        return idToVersionsRef == null ? null : idToVersionsRef.get();

    }

    @Override
    public void refresh() {
        onDisconnect();
        updateState(lastKnownUrl.get());
    }
}
