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

package org.apache.ignite.internal.table.distributed.schema;

import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.apache.ignite.internal.raft.util.OptimizedMarshaller.NO_POOL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.table.distributed.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.core.State;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CheckCatalogVersionOnActionRequestTest extends BaseIgniteAbstractTest {
    private final ReplicaMessagesFactory replicaMessagesFactory = new ReplicaMessagesFactory();

    private final PartitionReplicationMessagesFactory tableMessagesFactory = new PartitionReplicationMessagesFactory();

    private final RaftMessagesFactory raftMessagesFactory = new RaftMessagesFactory();

    @Mock
    private CatalogService catalogService;

    @InjectMocks
    private CheckCatalogVersionOnActionRequest interceptor;

    @Mock
    private RpcContext rpcContext;

    @Mock
    private NodeManager nodeManager;

    @Mock
    private Node node;

    private final RaftOptions raftOptions = new RaftOptions();

    private final PeerId leaderId = new PeerId("leader");

    private PartitionCommandsMarshallerImpl commandsMarshaller;

    @BeforeEach
    void initMocks() {
        when(rpcContext.getNodeManager()).thenReturn(nodeManager);
        when(nodeManager.get(anyString(), any())).thenReturn(node);

        lenient().when(node.getRaftOptions()).thenReturn(raftOptions);
        lenient().when(node.getNodeState()).thenReturn(State.STATE_LEADER);
        lenient().when(node.getLeaderId()).thenReturn(leaderId);

        commandsMarshaller = new PartitionCommandsMarshallerImpl(defaultSerializationRegistry(), NO_POOL);
    }

    @Test
    void delegatesWhenCommandHasNoRequiredCatalogVersion() {
        ActionRequest request = raftMessagesFactory.writeActionRequest()
                .groupId("test")
                .command(commandsMarshaller.marshall(commandWithoutRequiredCatalogVersion()))
                .build();

        assertThat(interceptor.intercept(rpcContext, request, commandsMarshaller), is(nullValue()));
    }

    private WriteCommand commandWithoutRequiredCatalogVersion() {
        return replicaMessagesFactory.safeTimeSyncCommand().build();
    }

    @Test
    void delegatesWhenHavingEnoughMetadata() {
        when(catalogService.latestCatalogVersion()).thenReturn(5);

        ActionRequest request = raftMessagesFactory.writeActionRequest()
                .groupId("test")
                .command(commandsMarshaller.marshall(commandWithRequiredCatalogVersion(3)))
                .build();

        assertThat(interceptor.intercept(rpcContext, request, commandsMarshaller), is(nullValue()));
    }

    private WriteCommand commandWithRequiredCatalogVersion(int requiredVersion) {
        return tableMessagesFactory.updateCommand()
                .tablePartitionId(tableMessagesFactory.tablePartitionIdMessage().build())
                .txId(UUID.randomUUID())
                .rowUuid(UUID.randomUUID())
                .txCoordinatorId("coordinator")
                .requiredCatalogVersion(requiredVersion)
                .build();
    }

    @Test
    void returnsErrorCodeBusyWhenNotHavingEnoughMetadata() {
        when(catalogService.latestCatalogVersion()).thenReturn(5);

        ActionRequest request = raftMessagesFactory.writeActionRequest()
                .groupId("test")
                .command(commandsMarshaller.marshall(commandWithRequiredCatalogVersion(6)))
                .build();

        Message result = interceptor.intercept(rpcContext, request, commandsMarshaller);

        assertThat(result, is(notNullValue()));
        assertThat(result, instanceOf(ErrorResponse.class));

        ErrorResponse errorResponse = (ErrorResponse) result;
        assertThat(errorResponse.errorCode(), is(RaftError.EBUSY.getNumber()));
        assertThat(errorResponse.errorMsg(),
                is("Metadata not yet available, rejecting ActionRequest with EBUSY [group=test, requiredLevel=6]."));
    }

    @ParameterizedTest
    @MethodSource("notLeaderNotTransferringStates")
    void checksLeadershipBeforeCheckingMetadataWhenNotLeaderAndNotTransferring(State state) {
        when(node.getNodeState()).thenReturn(state);

        ActionRequest request = raftMessagesFactory.writeActionRequest()
                .groupId("test")
                .command(commandsMarshaller.marshall(commandWithRequiredCatalogVersion(6)))
                .build();

        Message result = interceptor.intercept(rpcContext, request, commandsMarshaller);

        assertThat(result, is(notNullValue()));
        assertThat(result, instanceOf(ErrorResponse.class));

        ErrorResponse errorResponse = (ErrorResponse) result;
        assertThat(errorResponse.errorCode(), is(RaftError.EPERM.getNumber()));
        assertThat(errorResponse.errorMsg(), is("Is not leader."));
        assertThat(errorResponse.leaderId(), is(leaderId.toString()));
    }

    private static Stream<Arguments> notLeaderNotTransferringStates() {
        return Arrays.stream(State.values())
                .filter(state -> state != State.STATE_LEADER && state != State.STATE_TRANSFERRING)
                .map(Arguments::of);
    }

    @Test
    void checksLeadershipBeforeCheckingMetadataWhenTransferring() {
        when(node.getNodeState()).thenReturn(State.STATE_TRANSFERRING);

        ActionRequest request = raftMessagesFactory.writeActionRequest()
                .groupId("test")
                .command(commandsMarshaller.marshall(commandWithRequiredCatalogVersion(6)))
                .build();

        Message result = interceptor.intercept(rpcContext, request, commandsMarshaller);

        assertThat(result, is(notNullValue()));
        assertThat(result, instanceOf(ErrorResponse.class));

        ErrorResponse errorResponse = (ErrorResponse) result;
        assertThat(errorResponse.errorCode(), is(RaftError.EBUSY.getNumber()));
        assertThat(errorResponse.errorMsg(), is("Is transferring leadership."));
        assertThat(errorResponse.leaderId(), is(nullValue()));
    }
}
