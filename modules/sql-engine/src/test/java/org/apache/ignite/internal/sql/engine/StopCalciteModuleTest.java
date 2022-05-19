/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.internal.schema.SchemaManager.INITIAL_SCHEMA_VERSION;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.engine.exec.ExecutionCancelledException;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
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
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(MockitoExtension.class)
public class StopCalciteModuleTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(StopCalciteModuleTest.class);

    private static final int ROWS = 5;

    private static final String NODE_NAME = "mock-node-name";

    @Mock
    ClusterService clusterSrvc;

    @Mock
    TableManager tableManager;

    @Mock
    DataStorageManager dataStorageManager;

    @Mock
    MessagingService msgSrvc;

    @Mock
    TopologyService topologySrvc;

    @Mock
    InternalTable tbl;

    SchemaRegistry schemaReg;

    TestRevisionRegister testRevisionRegister = new TestRevisionRegister();

    @InjectConfiguration(polymorphicExtensions = {HashIndexConfigurationSchema.class, UnknownDataStorageConfigurationSchema.class})
    TablesConfiguration tablesConfig;

    /**
     * Before.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @BeforeEach
    public void before(TestInfo testInfo) {
        when(clusterSrvc.messagingService()).thenReturn(msgSrvc);
        when(clusterSrvc.topologyService()).thenReturn(topologySrvc);

        ClusterNode node = new ClusterNode("mock-node-id", NODE_NAME, null);
        when(topologySrvc.localMember()).thenReturn(node);
        when(topologySrvc.allMembers()).thenReturn(Collections.singleton(node));

        SchemaDescriptor schemaDesc = new SchemaDescriptor(
                1,
                new Column[]{new Column(0, "ID", NativeTypes.INT32, false)},
                new Column[]{new Column(1, "VAL", NativeTypes.INT32, false)}
        );

        schemaReg = new SchemaRegistryImpl((v) -> schemaDesc, () -> INITIAL_SCHEMA_VERSION, schemaDesc);

        when(tbl.name()).thenReturn("PUBLIC.TEST");

        // Mock create table (notify on register listener).
        doAnswer(invocation -> {
            EventListener<TableEventParameters> clo = (EventListener<TableEventParameters>) invocation.getArguments()[1];

            clo.notify(new TableEventParameters(0, UUID.randomUUID(), "TEST", new TableImpl(tbl, schemaReg)),
                    null);

            return null;
        }).when(tableManager).listen(eq(TableEvent.CREATE), any());

        RowAssembler asm = new RowAssembler(schemaReg.schema(), 0, 0, 0, 0);

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
        }).when(tbl).scan(anyInt(), any());

        LOG.info(">>>> Starting test {}", testInfo.getTestMethod().orElseThrow().getName());
    }

    @Test
    public void testStopQueryOnNodeStop() throws Exception {
        SqlQueryProcessor qryProc = new SqlQueryProcessor(
                testRevisionRegister,
                clusterSrvc,
                tableManager,
                dataStorageManager,
                Map::of
        );

        when(tbl.tableId()).thenReturn(UUID.randomUUID());

        when(tbl.storage()).thenReturn(mock(TableStorage.class));
        when(tbl.storage().configuration()).thenReturn(mock(TableConfiguration.class));
        when(tbl.storage().configuration().partitions()).thenReturn(mock(ConfigurationValue.class));
        when(tbl.storage().configuration().partitions().value()).thenReturn(1);

        qryProc.start();

        testRevisionRegister.moveRevision.apply(0L).join();

        var cursors = qryProc.queryAsync(
                "PUBLIC",
                "SELECT * FROM TEST"
        );

        await(cursors.get(0).thenCompose(cursor -> cursor.requestNextAsync(1)));

        assertTrue(isThereNodeThreads(NODE_NAME));

        qryProc.stop();

        var request = cursors.get(0)
                .thenCompose(cursor -> cursor.requestNextAsync(1));

        // Check cursor closed.
        await(request.exceptionally(t -> {
            assertInstanceOf(CompletionException.class, t);
            assertInstanceOf(IgniteException.class, t.getCause());
            assertInstanceOf(ExecutionCancelledException.class, t.getCause().getCause());

            return null;
        }));
        assertTrue(request.isCompletedExceptionally());

        // Check execute query on stopped node.
        assertTrue(assertThrows(IgniteInternalException.class, () -> qryProc.queryAsync(
                "PUBLIC",
                "SELECT 1"
        )).getCause() instanceof NodeStoppingException);

        System.gc();

        // Check: there are no alive Ignite threads.
        assertFalse(isThereNodeThreads(NODE_NAME));
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
    private static class TestRevisionRegister implements Consumer<Function<Long, CompletableFuture<?>>> {
        /** Revision consumer. */
        Function<Long, CompletableFuture<?>> moveRevision;

        /** {@inheritDoc} */
        @Override
        public void accept(Function<Long, CompletableFuture<?>> function) {
            if (moveRevision == null) {
                moveRevision = function;
            } else {
                Function<Long, CompletableFuture<?>> old = moveRevision;

                moveRevision = rev -> allOf(
                    old.apply(rev),
                    function.apply(rev)
                );
            }
        }
    }
}
