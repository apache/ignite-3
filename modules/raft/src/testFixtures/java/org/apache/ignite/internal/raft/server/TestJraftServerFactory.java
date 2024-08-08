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

package org.apache.ignite.internal.raft.server;

import static org.apache.ignite.internal.configuration.IgnitePaths.partitionsPath;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;

/** Utilities for creating JRaftServer instances. */
public class TestJraftServerFactory {
    private TestJraftServerFactory() {
        // Intentionally left blank.
    }

    /**
     * Factory method for {@link JraftServerImpl}.
     * Uses the default logStorageFactory, {@link SharedLogStorageFactoryUtils#create(String, Path)},
     * and automatically wraps it in the JraftServerImpl instance start/stop methods.
     *
     * @param service Cluster service.
     * @param dataPath Data path.
     */
    public static JraftServerImpl create(ClusterService service, Path dataPath, SystemLocalConfiguration systemConfiguration) {
        return create(service, dataPath, systemConfiguration, new NodeOptions(), new RaftGroupEventsClientListener());
    }

    /**
     * Factory method for {@link JraftServerImpl}.
     * Uses the default logStorageFactory, {@link SharedLogStorageFactoryUtils#create(String, Path)},
     * and automatically wraps it in the JraftServerImpl instance start/stop methods.
     *
     * @param service Cluster service.
     * @param dataPath Data path.
     * @param opts Node Options.
     */
    public static JraftServerImpl create(
            ClusterService service,
            Path dataPath,
            SystemLocalConfiguration systemConfiguration,
            NodeOptions opts
    ) {
        return create(service, dataPath, systemConfiguration, opts, new RaftGroupEventsClientListener());
    }

    /**
     * Factory method for {@link JraftServerImpl}.
     * Uses the default logStorageFactory, {@link SharedLogStorageFactoryUtils#create(String, Path)},
     * and automatically wraps it in the JraftServerImpl instance start/stop methods.
     *
     * @param service Cluster service.
     * @param dataPath Data path.
     * @param opts Default node options.
     */
    public static JraftServerImpl create(
            ClusterService service,
            Path dataPath,
            SystemLocalConfiguration systemConfiguration,
            NodeOptions opts,
            RaftGroupEventsClientListener raftGroupEventsClientListener
    ) {
        ComponentWorkingDir partitionsWorkDir = partitionsPath(systemConfiguration, dataPath);

        LogStorageFactory defaultLogStorageFactory = SharedLogStorageFactoryUtils.create(
                service.nodeName(),
                partitionsWorkDir.raftLogPath()
        );
        return new JraftServerImpl(
                service,
                partitionsWorkDir.metaPath(),
                opts,
                raftGroupEventsClientListener,
                defaultLogStorageFactory
        ) {
            @Override
            public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                return defaultLogStorageFactory.startAsync(componentContext).thenCompose(none -> super.startAsync(componentContext));
            }

            @Override
            public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                return super.stopAsync(componentContext).thenCompose(none -> defaultLogStorageFactory.stopAsync(componentContext));
            }
        };
    }
}
