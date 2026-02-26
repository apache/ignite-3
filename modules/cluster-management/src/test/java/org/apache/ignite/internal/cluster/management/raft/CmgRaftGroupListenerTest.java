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

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.LongConsumer;
import org.apache.ignite.internal.cluster.management.ClusterIdHolder;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.raft.commands.ClusterNodeMessage;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for the {@link CmgRaftGroupListener}.
 */
@ExtendWith(MockitoExtension.class)
public class CmgRaftGroupListenerTest extends BaseIgniteAbstractTest {
    private final ClusterStateStorage storage = spy(new TestClusterStateStorage());

    @Mock
    private LongConsumer onLogicalTopologyChanged;

    @Spy
    private final LogicalTopology logicalTopology = new LogicalTopologyImpl(storage, new NoOpFailureManager());

    private CmgRaftGroupListener listener;

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    private final ClusterTag clusterTag = ClusterTag.randomClusterTag(msgFactory, "cluster");

    private final ClusterState state = msgFactory.clusterState()
            .cmgNodes(Set.of("foo"))
            .metaStorageNodes(Set.of("bar"))
            .version(IgniteProductVersion.CURRENT_VERSION.toString())
            .clusterTag(clusterTag)
            .initialClusterConfiguration("some-config")
            .formerClusterIds(List.of(randomUUID(), randomUUID()))
            .build();

    private final ClusterNodeMessage node = msgFactory.clusterNodeMessage()
            .id(randomUUID())
            .name("bar")
            .host("localhost")
            .port(666)
            .build();

    private final ClusterIdHolder clusterIdHolder = new ClusterIdHolder();

    @BeforeEach
    void setUp() {
        assertThat(storage.startAsync(new ComponentContext()), willCompleteSuccessfully());

        var clusterStateStorageMgr = new ClusterStateStorageManager(storage);
        var validationManager = new ValidationManager(clusterStateStorageMgr, logicalTopology);

        listener = new CmgRaftGroupListener(
                clusterStateStorageMgr,
                logicalTopology,
                validationManager,
                onLogicalTopologyChanged,
                clusterIdHolder,
                new NoOpFailureManager(),
                config -> {}
        );
    }

    @AfterEach
    void tearDown() {
        assertThat(storage.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    /**
     * Test that validated nodes get added and removed from the storage.
     */
    @Test
    void testValidatedNodes() {
        listener.onWrite(iterator(msgFactory.initCmgStateCommand().node(node).clusterState(state).build()));

        listener.onWrite(iterator(msgFactory.joinRequestCommand().node(node).version(state.version()).clusterTag(clusterTag).build()));

        assertThat(listener.storageManager().getValidatedNodes(), contains(new LogicalNode(node.asClusterNode())));

        listener.onWrite(iterator(msgFactory.joinReadyCommand().node(node).build()));

        assertThat(listener.storageManager().getValidatedNodes(), is(empty()));
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
    void changeClusterNameChangesClusterName() {
        initCmgAndChangeClusterName();

        ClusterState updatedState = listener.storageManager().getClusterState();
        assertThat(updatedState, is(notNullValue()));

        assertThat(updatedState.cmgNodes(), is(state.cmgNodes()));
        assertThat(updatedState.metaStorageNodes(), is(state.metaStorageNodes()));
        assertThat(updatedState.clusterTag().clusterId(), is(state.clusterTag().clusterId()));
        assertThat(updatedState.clusterTag().clusterName(), is("cluster2"));
        assertThat(updatedState.version(), is(state.version()));
        assertThat(updatedState.initialClusterConfiguration(), is(state.initialClusterConfiguration()));
        assertThat(updatedState.formerClusterIds(), is(state.formerClusterIds()));
    }

    @Test
    void changeMetastorageInfoChangesMsInfo() {
        initCmgAndChangeMgInfo();

        ClusterState updatedState = listener.storageManager().getClusterState();
        assertThat(updatedState, is(notNullValue()));

        assertThat(updatedState.cmgNodes(), is(state.cmgNodes()));
        assertThat(updatedState.metaStorageNodes(), containsInAnyOrder("new-ms-1", "new-ms-2"));
        assertThat(updatedState.clusterTag(), is(state.clusterTag()));
        assertThat(updatedState.version(), is(state.version()));
        assertThat(updatedState.initialClusterConfiguration(), is(state.initialClusterConfiguration()));
        assertThat(updatedState.formerClusterIds(), is(state.formerClusterIds()));

        assertThat(listener.storageManager().getMetastorageRepairingConfigIndex(), is(123L));
    }

    private void initCmgAndChangeClusterName() {
        listener.onWrite(iterator(
                msgFactory.initCmgStateCommand()
                        .clusterState(state)
                        .node(node)
                        .build()
        ));

        listener.onWrite(iterator(
                msgFactory.changeClusterNameCommand()
                        .clusterName("cluster2")
                        .build()
        ));
    }

    private void initCmgAndChangeMgInfo() {
        listener.onWrite(iterator(
                msgFactory.initCmgStateCommand()
                        .clusterState(state)
                        .node(node)
                        .build()
        ));

        listener.onWrite(iterator(
                msgFactory.changeMetaStorageInfoCommand()
                        .metaStorageNodes(Set.of("new-ms-1", "new-ms-2"))
                        .metastorageRepairingConfigIndex(123L)
                        .build()
        ));
    }

    @Test
    void initStoresClusterId() {
        listener.onWrite(iterator(
                msgFactory.initCmgStateCommand()
                        .clusterState(state)
                        .node(node)
                        .build()
        ));

        assertThat(clusterIdHolder.clusterId(), is(state.clusterTag().clusterId()));
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
