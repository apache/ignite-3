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

package org.apache.ignite.internal.sql.engine;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.schema.SchemaManager.INITIAL_SCHEMA_VERSION;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.engine.exec.ExecutionCancelledException;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest.TestHashIndex;
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Stop Calcite module test.
 */
@ExtendWith(MockitoExtension.class)
public class StopCalciteModuleTest {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(StopCalciteModuleTest.class);

    private static final int ROWS = 5;

    private static final String NODE_NAME = "mock-node-name";

    @Mock
    private ClusterService clusterSrvc;

    @Mock
    private TableManager tableManager;

    @Mock
    private IndexManager indexManager;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private DataStorageManager dataStorageManager;

    @Mock
    private MessagingService msgSrvc;

    @Mock
    private TxManager txManager;

    @Mock
    private DistributionZoneManager distributionZoneManager;

    @Mock
    private TopologyService topologySrvc;

    @Mock
    private InternalTable tbl;

    @Mock
    private HybridClock clock;

    @Mock
    private CatalogManager catalogManager;

    private SchemaRegistry schemaReg;

    private final TestRevisionRegister testRevisionRegister = new TestRevisionRegister();

    private final ClusterNode localNode = new ClusterNode("mock-node-id", NODE_NAME, null);

    private final MetricManager metricManager = new MetricManager();

    private final int tblId = 1;

    /**
     * Before.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @BeforeEach
    public void before(TestInfo testInfo) {
        when(clusterSrvc.messagingService()).thenReturn(msgSrvc);
        when(clusterSrvc.topologyService()).thenReturn(topologySrvc);
        when(topologySrvc.localMember()).thenReturn(localNode);
        when(topologySrvc.allMembers()).thenReturn(List.of(localNode));

        SchemaDescriptor schemaDesc = new SchemaDescriptor(
                1,
                new Column[]{new Column(0, "ID", NativeTypes.INT32, false)},
                new Column[]{new Column(1, "VAL", NativeTypes.INT32, false)}
        );

        schemaReg = new SchemaRegistryImpl((v) -> completedFuture(schemaDesc), () -> completedFuture(INITIAL_SCHEMA_VERSION), schemaDesc);

        when(tbl.name()).thenReturn("TEST");

        when(schemaManager.schemaRegistry(anyLong(), anyInt())).thenReturn(completedFuture(schemaReg));

        // Mock create table (notify on register listener).
        doAnswer(invocation -> {
            EventListener<TableEventParameters> clo = (EventListener<TableEventParameters>) invocation.getArguments()[1];

            clo.notify(new TableEventParameters(0, tblId), null);

            return null;
        }).when(tableManager).listen(eq(TableEvent.CREATE), any());

        doAnswer(invocation -> {
            EventListener<IndexEventParameters> clo = (EventListener<IndexEventParameters>) invocation.getArguments()[1];

            TestHashIndex testHashIndex = TestHashIndex.create(List.of("ID"), "pk_idx", tblId);

            clo.notify(new IndexEventParameters(0, testHashIndex.tableId(), testHashIndex.id(), testHashIndex.descriptor()), null);

            return null;
        }).when(indexManager).listen(eq(IndexEvent.CREATE), any());

        RowAssembler asm = new RowAssembler(schemaReg.schema());

        asm.appendInt(0);
        asm.appendInt(0);

        BinaryRow binaryRow = asm.build();

        // Mock table scan
        doAnswer(invocation -> {
            int part = (int) invocation.getArguments()[0];

            return (Flow.Publisher<BinaryRow>) s -> {
                s.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long n) {
                        // No-op.
                    }

                    @Override
                    public void cancel() {
                        // No-op.
                    }
                });

                if (part == 0) {
                    for (int i = 0; i < ROWS; ++i) {
                        s.onNext(binaryRow);
                    }
                }

                s.onComplete();
            };
        }).when(tbl).scan(anyInt(), any(), any());

        LOG.info(">>>> Starting test {}", testInfo.getTestMethod().orElseThrow().getName());
    }

    @Test
    public void testStopQueryOnNodeStop() throws Exception {
        SqlQueryProcessor qryProc = new SqlQueryProcessor(
                testRevisionRegister,
                clusterSrvc,
                tableManager,
                indexManager,
                schemaManager,
                dataStorageManager,
                txManager,
                distributionZoneManager,
                Map::of,
                mock(ReplicaService.class),
                clock,
                catalogManager,
                metricManager
        );

        TableImpl tableImpl = new TableImpl(tbl, schemaReg, new HeapLockManager());
        when(tableManager.tableAsync(anyLong(), eq(tblId))).thenReturn(completedFuture(tableImpl));
        when(tableManager.tableAsync(eq(tblId))).thenReturn(completedFuture(tableImpl));

        when(schemaManager.schemaRegistry(eq(tblId))).thenReturn(schemaReg);

        when(tbl.tableId()).thenReturn(tblId);
        when(tbl.primaryReplicas()).thenReturn(completedFuture(List.of(new PrimaryReplica(localNode, -1L))));

        when(tbl.storage()).thenReturn(mock(MvTableStorage.class));
        when(tbl.storage().getTableDescriptor()).thenReturn(new StorageTableDescriptor(tblId, 1, "none"));

        when(txManager.begin(anyBoolean(), any())).thenReturn(new NoOpTransaction(localNode.name()));

        qryProc.start();

        await(testRevisionRegister.moveRevision.apply(0L));

        SessionId sessionId = qryProc.createSession(PropertiesHelper.emptyHolder());
        QueryContext context = QueryContext.create(SqlQueryType.ALL);

        var cursors = qryProc.querySingleAsync(
                sessionId,
                context,
                "SELECT * FROM TEST"
        );

        await(cursors.thenCompose(cursor -> cursor.requestNextAsync(1)));

        assertTrue(isThereNodeThreads(NODE_NAME));

        qryProc.stop();

        var request = cursors.thenCompose(cursor -> cursor.requestNextAsync(1));

        // Check cursor closed.
        await(request.exceptionally(t -> {
            assertInstanceOf(CompletionException.class, t);
            assertInstanceOf(IgniteException.class, t.getCause());
            assertInstanceOf(ExecutionCancelledException.class, t.getCause().getCause());

            return null;
        }));
        assertTrue(request.isCompletedExceptionally());

        // Check execute query on stopped node.
        assertTrue(assertThrows(IgniteInternalException.class, () -> qryProc.querySingleAsync(
                sessionId,
                context,
                "SELECT 1"
        )).getCause() instanceof NodeStoppingException);

        System.gc();

        assertTrue(IgniteTestUtils.waitForCondition(() -> !isThereNodeThreads(NODE_NAME), 1000));
    }

    /**
     * Get isThereNodeThreads flag.
     *
     * @return {@code true} is there are any threads with node name prefix; Otherwise returns {@code false}.
     */
    private boolean isThereNodeThreads(String nodeName) {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = bean.dumpAllThreads(true, true);

        return Arrays.stream(infos)
                .anyMatch((ti) -> ti.getThreadName().contains(nodeName));
    }

    /**
     * Test revision register.
     */
    private static class TestRevisionRegister implements Consumer<LongFunction<CompletableFuture<?>>> {
        /** Revision consumer. */
        LongFunction<CompletableFuture<?>> moveRevision;

        /** {@inheritDoc} */
        @Override
        public void accept(LongFunction<CompletableFuture<?>> function) {
            if (moveRevision == null) {
                moveRevision = function;
            } else {
                LongFunction<CompletableFuture<?>> old = moveRevision;

                moveRevision = rev -> allOf(
                        old.apply(rev),
                        function.apply(rev)
                );
            }
        }
    }
}
