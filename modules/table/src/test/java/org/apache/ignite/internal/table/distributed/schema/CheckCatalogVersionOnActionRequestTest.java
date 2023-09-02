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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CheckCatalogVersionOnActionRequestTest extends BaseIgniteAbstractTest {
    private final ReplicaMessagesFactory replicaMessagesFactory = new ReplicaMessagesFactory();

    private final TableMessagesFactory tableMessagesFactory = new TableMessagesFactory();

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

    @BeforeEach
    void initMocks() {
        when(rpcContext.getNodeManager()).thenReturn(nodeManager);
        when(nodeManager.get(anyString(), any())).thenReturn(node);
        lenient().when(node.getRaftOptions()).thenReturn(raftOptions);
    }

    @Test
    void delegatesWhenCommandHasNoRequiredCatalogVersion() {
        ActionRequest request = raftMessagesFactory.actionRequest()
                .groupId("test")
                .command(commandWithoutRequiredCatalogVersion())
                .build();

        assertThat(interceptor.intercept(rpcContext, request), is(nullValue()));
    }

    private Command commandWithoutRequiredCatalogVersion() {
        return replicaMessagesFactory.safeTimeSyncCommand().build();
    }

    @Test
    void delegatesWhenHavingEnoughMetadata() {
        when(catalogService.latestCatalogVersion()).thenReturn(5);

        ActionRequest request = raftMessagesFactory.actionRequest()
                .groupId("test")
                .command(commandWithRequiredCatalogVersion(3))
                .build();

        assertThat(interceptor.intercept(rpcContext, request), is(nullValue()));
    }

    private Command commandWithRequiredCatalogVersion(int requiredVersion) {
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

        ActionRequest request = raftMessagesFactory.actionRequest()
                .groupId("test")
                .command(commandWithRequiredCatalogVersion(6))
                .build();

        Message result = interceptor.intercept(rpcContext, request);

        assertThat(result, is(notNullValue()));
        assertThat(result, instanceOf(ErrorResponse.class));

        ErrorResponse errorResponse = (ErrorResponse) result;
        assertThat(errorResponse.errorCode(), is(RaftError.EBUSY.getNumber()));
        assertThat(errorResponse.errorMsg(),
                is("Metadata not yet available, group 'test', required level 6; rejecting ActionRequest with EBUSY."));
    }
}
