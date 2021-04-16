/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.metastorage.internal;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.internal.Loza;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MetaStorageManager {
    private final ClusterService clusterNetSvc;

    private final Loza raftMgr;

    private MetaStorageService service;

    public MetaStorageManager(
        ClusterService clusterNetSvc,
        Loza raftMgr,
        ConfigurationManager locConfigurationMgr
    ) {
        this.clusterNetSvc = clusterNetSvc;
        this.raftMgr = raftMgr;

        clusterNetSvc.messagingService().addMessageHandler((message, sender, correlationId) -> {
            // TODO: IGNITE-14414 Cluster initialization flow.
        });
    }

    public synchronized CompletableFuture<Long> registerWatch(@Nullable Key key, @NotNull WatchListener lsnr) {
        // TODO: IGNITE-14446 Implement DMS manager with watch registry.
        return null;
    }

    public synchronized void deployWatches() {
        // TODO: IGNITE-14446 Implement DMS manager with watch registry.
    }
}

