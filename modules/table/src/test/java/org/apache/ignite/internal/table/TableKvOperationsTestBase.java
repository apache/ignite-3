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

package org.apache.ignite.internal.table;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.distributed.schema.ConstantSchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Basic table operations test.
 */
@ExtendWith(MockitoExtension.class)
abstract class TableKvOperationsTestBase extends BaseIgniteAbstractTest {
    @Mock(answer = RETURNS_DEEP_STUBS)
    protected ReplicaService replicaService;

    protected TxManager txManager;

    @Mock
    protected HybridTimestampTracker observableTimestampTracker;

    protected final int schemaVersion = 1;

    private final UUID txId = UUID.randomUUID();

    protected final SchemaVersions schemaVersions = new ConstantSchemaVersions(schemaVersion);

    @BeforeEach
    void setUp() {
        txManager = DummyInternalTableImpl.txManager(replicaService);
    }

    protected final TableImpl createTable(SchemaDescriptor schema) {
        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.topologyService().localMember().address())
                .thenReturn(DummyInternalTableImpl.ADDR);

        var internalTable = createInternalTable(schema);

        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class, RETURNS_DEEP_STUBS));

        return new TableImpl(
                internalTable,
                new DummySchemaManagerImpl(schema),
                new HeapLockManager(),
                txManager,
                observableTimestampTracker,
                schemaVersions
        );
    }

    protected final DummyInternalTableImpl createInternalTable(SchemaDescriptor schema) {
        return new DummyInternalTableImpl(
                replicaService,
                txManager,
                false,
                null,
                schema,
                observableTimestampTracker
        );
    }
}
