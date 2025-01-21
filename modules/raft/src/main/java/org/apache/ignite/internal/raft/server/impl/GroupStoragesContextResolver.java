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
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.DestroyStorageContext;
import org.apache.ignite.internal.raft.storage.impl.DestroyStorageIntent;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/** Resolves {@link LogStorageFactory} and server data path for given raft node or {@link DestroyStorageIntent}. */
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
        assert serverDataPathByGroupName.size() == 3 : "CMG, Metastorage and raft partition groups must be present";
        assert logStorageFactoryByGroupName.size() == 3 : "CMG, Metastorage and raft partition groups must be present";

        this.groupNameResolver = groupNameResolver;
        this.serverDataPathByGroupName = serverDataPathByGroupName;
        this.logStorageFactoryByGroupName = logStorageFactoryByGroupName;
    }

    DestroyStorageContext getContext(DestroyStorageIntent intent) {
        LogStorageFactory logStorageFactory = intent.isVolatile() ? null : logStorageFactoryByGroupName.get(intent.groupName());
        return new DestroyStorageContext(
                intent,
                logStorageFactory,
                serverDataPathByGroupName.get(intent.groupName())
        );
    }

    DestroyStorageContext getContext(RaftNodeId nodeId, LogStorageFactory logStorageFactory, boolean isVolatile) {
        DestroyStorageIntent intent = new DestroyStorageIntent(
                nodeId.nodeIdStringForStorage(),
                groupNameResolver.apply(nodeId.groupId()),
                isVolatile
        );

        return new DestroyStorageContext(intent, logStorageFactory, serverDataPathByGroupName.get(intent.groupName()));
    }
}
