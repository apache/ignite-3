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
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterService;

class Node implements AutoCloseable {
    final String name;

    final VaultManager vault;

    final ClusterService clusterService;

    final Loza loza;

    final MetaStorageManagerImpl metastore;

    final PlacementDriverManager placementDriverManager;

    Node(
            String name,
            VaultManager vault,
            ClusterService clusterService,
            Loza loza,
            MetaStorageManagerImpl metastore,
            PlacementDriverManager placementDriverManager
    ) {
        this.name = name;
        this.vault = vault;
        this.clusterService = clusterService;
        this.loza = loza;
        this.metastore = metastore;
        this.placementDriverManager = placementDriverManager;
    }

    void start() {
        vault.start();
        clusterService.start();
        loza.start();
        metastore.start();

        assertThat(metastore.recoveryFinishedFuture(), willCompleteSuccessfully());

        placementDriverManager.start();

        assertThat(metastore.notifyRevisionUpdateListenerOnStart(), willCompleteSuccessfully());
        assertThat(metastore.deployWatches(), willCompleteSuccessfully());
    }

    @Override
    public void close() throws Exception {
        List<IgniteComponent> igniteComponents = List.of(placementDriverManager, metastore, loza, clusterService, vault);

        IgniteUtils.closeAll(Stream.concat(
                igniteComponents.stream().map(component -> component::beforeNodeStop),
                Stream.of(() -> IgniteUtils.stopAll(igniteComponents.stream()))
        ));
    }
}
