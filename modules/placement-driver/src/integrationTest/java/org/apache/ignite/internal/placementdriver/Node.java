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

package org.apache.ignite.internal.placementdriver;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.util.IgniteUtils;

class Node implements AutoCloseable {
    final String name;

    final ClusterService clusterService;

    final Loza loza;

    LogStorageFactory partitionsLogStorageFactory;

    LogStorageFactory msLogStorageFactory;

    final MetaStorageManagerImpl metastore;

    final PlacementDriverManager placementDriverManager;

    Node(
            String name,
            ClusterService clusterService,
            Loza loza,
            LogStorageFactory partitionsLogStorageFactory,
            LogStorageFactory msLogStorageFactory,
            MetaStorageManagerImpl metastore,
            PlacementDriverManager placementDriverManager
    ) {
        this.name = name;
        this.clusterService = clusterService;
        this.loza = loza;
        this.metastore = metastore;
        this.placementDriverManager = placementDriverManager;
        this.partitionsLogStorageFactory = partitionsLogStorageFactory;
        this.msLogStorageFactory = msLogStorageFactory;
    }

    CompletableFuture<Void> startAsync() {
        ComponentContext componentContext = new ComponentContext();

        return IgniteUtils.startAsync(componentContext, clusterService, partitionsLogStorageFactory, msLogStorageFactory, loza, metastore)
                .thenCompose(unused -> metastore.recoveryFinishedFuture())
                .thenCompose(unused -> placementDriverManager.startAsync(componentContext))
                .thenCompose(unused -> metastore.notifyRevisionUpdateListenerOnStart())
                .thenCompose(unused -> metastore.deployWatches());
    }

    @Override
    public void close() throws Exception {
        List<IgniteComponent> igniteComponents =
                List.of(placementDriverManager, metastore, loza, msLogStorageFactory, partitionsLogStorageFactory, clusterService);

        closeAll(Stream.concat(
                igniteComponents.stream().map(component -> component::beforeNodeStop),
                Stream.of(() -> assertThat(stopAsync(new ComponentContext(), igniteComponents), willCompleteSuccessfully()))
        ));
    }
}
