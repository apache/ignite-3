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

package org.apache.ignite.distributed;

import static org.apache.ignite.distributed.ItTxDistributedTestSingleNode.NODE_PORT_BASE;
import static org.apache.ignite.distributed.ItTxDistributedTestSingleNode.startNode;
import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests handling requests from {@link ReplicaService} to {@link ReplicaManager} when the {@link Replica}
 * is not started.
 */
public class ReplicaUnavailableTest extends IgniteAbstractTest {
    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT64, false)},
            new Column[]{new Column("value", NativeTypes.INT64, false)}
    );

    private final TableMessagesFactory tableMessagesFactory = new TableMessagesFactory();

    private final TestInfo testInfo;

    private ReplicaService replicaService;

    private ReplicaManager replicaManager;

    private ClusterService clusterService;

    private NetworkAddress networkAddress;

    private String name = "client";

    @BeforeEach
    public void setup() {
        networkAddress = new NetworkAddress(getLocalAddress(), NODE_PORT_BASE + 1);

        var nodeFinder = new StaticNodeFinder(List.of(networkAddress));

        clusterService = startNode(testInfo, name, NODE_PORT_BASE + 1, nodeFinder);

        HybridClock clock = mock(HybridClock.class);

        replicaService = new ReplicaService(clusterService.messagingService(), clock);

        replicaManager = new ReplicaManager(clusterService,
                clock,
                Set.of(TableMessageGroup.class, TxMessageGroup.class));

        replicaManager.start();
    }

    @AfterEach
    public void teardown() {
        clusterService.stop();
    }

    public ReplicaUnavailableTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18868")
    public void testWithReplicaStartedAfterRequestSending() throws Exception {
        ClusterNode clusterNode = clusterService.topologyService().localMember();

        TablePartitionId tablePartitionId = new TablePartitionId(UUID.randomUUID(), 1);

        ReadWriteSingleRowReplicaRequest request = tableMessagesFactory.readWriteSingleRowReplicaRequest()
                .groupId(tablePartitionId)
                .binaryRow(createKeyValueRow(1L, 1L))
                .requestType(RequestType.RW_GET)
                .build();

        clusterService.messagingService().addMessageHandler(ReplicaMessageGroup.class,
                (message, sender, correlationId) -> {
                    try {
                        replicaManager.startReplica(
                                tablePartitionId,
                                CompletableFuture.completedFuture(null),
                                request0 -> CompletableFuture.completedFuture(null),
                                mock(TopologyAwareRaftGroupService.class),
                                new PendingComparableValuesTracker<>(0L)
                        );
                    } catch (NodeStoppingException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        replicaService.invoke(clusterNode, request).get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testWithNotStartedReplica() {
        ClusterNode clusterNode = clusterService.topologyService().localMember();

        TablePartitionId tablePartitionId = new TablePartitionId(UUID.randomUUID(), 1);

        ReadWriteSingleRowReplicaRequest request = tableMessagesFactory.readWriteSingleRowReplicaRequest()
                .groupId(tablePartitionId)
                .binaryRow(createKeyValueRow(1L, 1L))
                .requestType(RequestType.RW_GET)
                .build();

        Exception e0 = null;
        Exception e1 = null;

        try {
            replicaService.invoke(clusterNode, request).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            e0 = e;
        }

        try {
            replicaService.invoke(clusterNode, request).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            e1 = e;
        }

        assertTrue(e0 != null);
        assertTrue(e0.getCause() instanceof ReplicaUnavailableException, e0.toString());

        assertTrue(e1 != null);
        assertTrue(e1.getCause() instanceof ReplicaUnavailableException, e1.toString());
    }

    private static Row createKeyValueRow(long id, long value) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA);

        rowBuilder.appendLong(id);
        rowBuilder.appendLong(value);

        return new Row(SCHEMA, rowBuilder.build());
    }
}
