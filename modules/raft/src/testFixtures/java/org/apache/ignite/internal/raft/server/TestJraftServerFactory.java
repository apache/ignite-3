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

import static org.mockito.Mockito.mock;

import java.util.Map;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.server.impl.GroupStoragesContextResolver;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.GroupStoragesDestructionIntents;
import org.apache.ignite.internal.raft.storage.impl.NoopGroupStoragesDestructionIntents;
import org.apache.ignite.internal.raft.storage.impl.StorageDestructionIntent;
import org.apache.ignite.internal.raft.storage.impl.StoragesDestructionContext;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;

/** Utilities for creating JRaftServer instances. */
public class TestJraftServerFactory {
    private TestJraftServerFactory() {
        // Intentionally left blank.
    }

    /**
     * Factory method for {@link JraftServerImpl}.
     *
     * @param service Cluster service.
     */
    public static JraftServerImpl create(ClusterService service) {
        return create(service, new NodeOptions(), new RaftGroupEventsClientListener());
    }

    /**
     * Factory method for {@link JraftServerImpl}.
     *
     * @param service Cluster service.
     * @param opts Node Options.
     */
    public static JraftServerImpl create(ClusterService service, NodeOptions opts) {
        return create(service, opts, new RaftGroupEventsClientListener());
    }

    /**
     * Factory method for {@link JraftServerImpl}.
     *
     * @param service Cluster service.
     * @param opts Node Options.
     * @param raftGroupEventsClientListener Raft events listener.
     */
    public static JraftServerImpl create(
            ClusterService service,
            NodeOptions opts,
            RaftGroupEventsClientListener raftGroupEventsClientListener
    ) {
        return create(
                service,
                opts,
                raftGroupEventsClientListener,
                new NoopGroupStoragesDestructionIntents(),
                new GroupStoragesContextResolver(Object::toString, Map.of(), Map.of())
        );
    }

    /**
     * Factory method for {@link JraftServerImpl}.
     *
     * @param service Cluster service.
     * @param opts Node Options.
     * @param raftGroupEventsClientListener Raft events listener.
     * @param groupStoragesDestructionIntents Storage to persist {@link StorageDestructionIntent}s.
     * @param groupStoragesContextResolver Resolver to get {@link StoragesDestructionContext}s for storage destruction.
     */
    public static JraftServerImpl create(
            ClusterService service,
            NodeOptions opts,
            RaftGroupEventsClientListener raftGroupEventsClientListener,
            GroupStoragesDestructionIntents groupStoragesDestructionIntents,
            GroupStoragesContextResolver groupStoragesContextResolver
    ) {
        return new JraftServerImpl(
                service,
                opts,
                raftGroupEventsClientListener,
                mock(FailureManager.class),
                groupStoragesDestructionIntents,
                groupStoragesContextResolver
        );
    }
}
