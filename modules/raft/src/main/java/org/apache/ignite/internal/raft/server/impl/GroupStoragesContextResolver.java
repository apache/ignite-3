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

package org.apache.ignite.internal.raft.server.impl;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.StorageDestructionIntent;
import org.apache.ignite.internal.raft.storage.impl.StoragesDestructionContext;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/** Resolves {@link LogStorageFactory} and server data path for given {@link StorageDestructionIntent}. */
public class GroupStoragesContextResolver {
    private final Function<ReplicationGroupId, String> groupNameResolver;

    private final Map<String, Path> serverDataPathByGroupName;
    private final Map<String, LogStorageFactory> logStorageFactoryByGroupName;

    /** Constructor. */
    public GroupStoragesContextResolver(
            Function<ReplicationGroupId, String> groupNameResolver,
            Map<String, Path> serverDataPathByGroupName,
            Map<String, LogStorageFactory> logStorageFactoryByGroupName
    ) {
        this.groupNameResolver = groupNameResolver;
        this.serverDataPathByGroupName = Map.copyOf(serverDataPathByGroupName);
        this.logStorageFactoryByGroupName = Map.copyOf(logStorageFactoryByGroupName);
    }

    StoragesDestructionContext getContext(StorageDestructionIntent intent) {
        LogStorageFactory logStorageFactory = intent.isVolatile() ? null : logStorageFactoryByGroupName.get(intent.groupName());

        return new StoragesDestructionContext(intent, logStorageFactory, serverDataPathByGroupName.get(intent.groupName()));
    }

    StorageDestructionIntent getIntent(RaftNodeId nodeId, boolean isVolatile) {
        return new StorageDestructionIntent(nodeId.nodeIdStringForStorage(), groupNameResolver.apply(nodeId.groupId()), isVolatile);
    }

    Collection<Path> serverDataPaths() {
        return List.copyOf(serverDataPathByGroupName.values());
    }

    Collection<LogStorageFactory> logStorageFactories() {
        return List.copyOf(logStorageFactoryByGroupName.values());
    }
}
