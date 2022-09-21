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

package org.apache.ignite.internal.cluster.management.raft;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.raft.commands.InitCmgStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link CmgRaftGroupListener}.
 */
public class CmgRaftGroupListenerTest {
    private final ClusterStateStorage storage = new ConcurrentMapClusterStateStorage();

    private final CmgRaftGroupListener listener = new CmgRaftGroupListener(storage);

    @BeforeEach
    void setUp() {
        storage.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        storage.close();
    }

    /**
     * Test that validated node IDs get added and removed from the storage.
     */
    @Test
    void testValidatedNodeIds() {
        var state = new ClusterState(
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                new ClusterTag("cluster")
        );

        var node = new ClusterNode("foo", "bar", new NetworkAddress("localhost", 666));

        listener.onWrite(iterator(new InitCmgStateCommand(node, state)));

        listener.onWrite(iterator(new JoinRequestCommand(node, state.igniteVersion(), state.clusterTag())));

        assertThat(listener.storage().getValidatedNodeIds(), contains(node.id()));

        listener.onWrite(iterator(new JoinReadyCommand(node)));

        assertThat(listener.storage().getValidatedNodeIds(), is(empty()));
    }

    private static <T extends Command> Iterator<CommandClosure<T>> iterator(T obj) {
        CommandClosure<T> closure = new CommandClosure<>() {
            @Override
            public T command() {
                return obj;
            }

            @Override
            public void result(@Nullable Serializable res) {
                // no-op.
            }
        };

        return List.of(closure).iterator();
    }
}
