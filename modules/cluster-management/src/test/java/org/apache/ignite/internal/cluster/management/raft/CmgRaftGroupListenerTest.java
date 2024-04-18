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

import static org.apache.ignite.internal.cluster.management.ClusterTag.clusterTag;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.LongConsumer;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.raft.commands.ClusterNodeMessage;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link CmgRaftGroupListener}.
 */
public class CmgRaftGroupListenerTest extends BaseIgniteAbstractTest {
    private final ClusterStateStorage storage = spy(new TestClusterStateStorage());

    private final LongConsumer onLogicalTopologyChanged = mock(LongConsumer.class);

    private final LogicalTopology logicalTopology = spy(new LogicalTopologyImpl(storage));

    private CmgRaftGroupListener listener;

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    private final ClusterTag clusterTag = clusterTag(msgFactory, "cluster");

    private final ClusterState state = msgFactory.clusterState()
            .cmgNodes(Set.copyOf(Set.of("foo")))
            .metaStorageNodes(Set.copyOf(Set.of("bar")))
            .version(IgniteProductVersion.CURRENT_VERSION.toString())
            .clusterTag(clusterTag)
            .build();

    private final ClusterNodeMessage node = msgFactory.clusterNodeMessage().id("foo").name("bar").host("localhost").port(666).build();

    @BeforeEach
    void setUp() {
        storage.start();

        listener = new CmgRaftGroupListener(storage, logicalTopology, onLogicalTopologyChanged);
    }

    @AfterEach
    void tearDown() throws Exception {
        storage.stop();
    }

    /**
     * Test that validated nodes get added and removed from the storage.
     */
    @Test
    void testValidatedNodes() {
        listener.onWrite(iterator(msgFactory.initCmgStateCommand().node(node).clusterState(state).build()));

        listener.onWrite(iterator(msgFactory.joinRequestCommand().node(node).version(state.version()).clusterTag(clusterTag).build()));

        assertThat(listener.storage().getValidatedNodes(), contains(new LogicalNode(node.asClusterNode())));

        listener.onWrite(iterator(msgFactory.joinReadyCommand().node(node).build()));

        assertThat(listener.storage().getValidatedNodes(), is(empty()));
    }

    @Test
    void successfulJoinReadyExecutesOnLogicalTopologyChanged() {
        listener.onWrite(iterator(msgFactory.initCmgStateCommand().node(node).clusterState(state).build()));

        JoinRequestCommand joinRequestCommand = msgFactory.joinRequestCommand()
                .node(node)
                .version(state.version())
                .clusterTag(state.clusterTag())
                .build();
        listener.onWrite(iterator(joinRequestCommand));

        listener.onWrite(iterator(msgFactory.joinReadyCommand().node(node).build()));

        verify(onLogicalTopologyChanged).accept(anyLong());
    }

    @Test
    void unsuccessfulJoinReadyDoesNotExecuteOnLogicalTopologyChanged() {
        listener.onWrite(iterator(msgFactory.joinReadyCommand().node(node).build()));

        verify(onLogicalTopologyChanged, never()).accept(anyLong());
    }

    @Test
    void nodesLeaveExecutesOnLogicalTopologyChanged() {
        listener.onWrite(iterator(msgFactory.nodesLeaveCommand().nodes(Set.of(node)).build()));

        verify(onLogicalTopologyChanged).accept(anyLong());
    }

    @Test
    void restoreFromSnapshotTriggersTopologyLeapEvent() {
        doNothing().when(storage).restoreSnapshot(any());

        assertTrue(listener.onSnapshotLoad(Paths.get("/unused")));

        verify(logicalTopology).fireTopologyLeap();
    }

    @Test
    void absentClusterConfigUpdateErasesClusterConfig() {
        ClusterState clusterState = msgFactory.clusterState()
                .cmgNodes(Set.copyOf(Set.of("foo")))
                .metaStorageNodes(Set.copyOf(Set.of("bar")))
                .version(IgniteProductVersion.CURRENT_VERSION.toString())
                .clusterTag(clusterTag)
                .initialClusterConfiguration("config")
                .build();

        listener.onWrite(iterator(msgFactory.initCmgStateCommand().node(node).clusterState(clusterState).build()));

        Collection<String> cmgNodes = clusterState.cmgNodes();
        Collection<String> msNodes = clusterState.metaStorageNodes();
        IgniteProductVersion igniteVersion = clusterState.igniteVersion();
        ClusterTag clusterTag1 = clusterState.clusterTag();
        ClusterState clusterStateToUpdate = msgFactory.clusterState()
                .cmgNodes(Set.copyOf(cmgNodes))
                .metaStorageNodes(Set.copyOf(msNodes))
                .version(igniteVersion.toString())
                .clusterTag(clusterTag1)
                .build();

        listener.onWrite(iterator(msgFactory.updateClusterStateCommand().clusterState(clusterStateToUpdate).build()));
        ClusterState updatedClusterState = listener.storage().getClusterState();
        assertAll(
                () -> assertNull(updatedClusterState.initialClusterConfiguration()),
                () -> assertEquals(updatedClusterState.cmgNodes(), clusterState.cmgNodes()),
                () -> assertEquals(updatedClusterState.metaStorageNodes(), clusterState.metaStorageNodes()),
                () -> assertEquals(updatedClusterState.version(), clusterState.version()),
                () -> assertEquals(updatedClusterState.igniteVersion(), clusterState.igniteVersion()),
                () -> assertEquals(updatedClusterState.clusterTag(), clusterState.clusterTag())
        );
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
