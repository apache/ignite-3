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

package org.apache.ignite.internal.sql.engine.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


/**
 * Tests for {@link ExecutionDependencyResolverImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class ExecutionDependencyResolverSelfTest extends AbstractPlannerTest {

    @Mock
    private ExecutableTableRegistry registry;

    @Mock(name = "table1")
    private ScannableTable table1;

    @Mock(name = "update1")
    private UpdatableTable update1;

    @Mock(name = "table2")
    private ScannableTable table2;

    @Mock(name = "update2")
    private UpdatableTable update2;

    /**
     * Table scan.
     */
    @Test
    public void testTableScan() {
        IgniteTable testTable1 = createTestTable("TEST1");
        IgniteTable testTable2 = createTestTable("TEST2");

        int t1Id = testTable1.id();
        int t2Id = testTable2.id();

        Tester tester = new Tester(createSchema(testTable1, testTable2));

        tester.setDependencies(t1Id, table1, update1);
        tester.setDependencies(t2Id, table2, update2);

        CompletableFuture<ResolvedDependencies> f = tester.resolveDependencies("SELECT * FROM test1 JOIN test2 ON test1.id = test2.id");

        ResolvedDependencies deps = f.join();
        tester.checkDependencies(deps, t1Id);
        tester.checkDependencies(deps, t2Id);

        verify(registry, times(1)).getTable(anyInt(), eq(t1Id));
        verify(registry, times(1)).getTable(anyInt(), eq(t2Id));
    }

    /**
     * Index scan.
     */
    @Test
    public void testIndexScan() {
        IgniteTable table = createTestTable("TEST1", addHashIndex("ID"));

        int t1Id = table.id();

        Tester tester = new Tester(createSchema(table));
        tester.setDependencies(t1Id, table1, update1);

        CompletableFuture<ResolvedDependencies> f = tester.resolveDependencies("SELECT * FROM test1 WHERE id=1");
        tester.checkDependencies(f.join(), t1Id);

        verify(registry, times(1)).getTable(anyInt(), eq(t1Id));
    }

    /**
     * DML.
     */
    @Test
    public void testModify() {
        IgniteTable testTable1 = createTestTable("TEST1");
        IgniteTable testTable2 = createTestTable("TEST2");

        int t1Id = testTable1.id();
        int t2Id = testTable2.id();

        Tester tester = new Tester(createSchema(testTable1, testTable2));

        tester.setDependencies(t1Id, table1, update1);
        tester.setDependencies(t2Id, table2, update2);

        CompletableFuture<ResolvedDependencies> f = tester.resolveDependencies(
                "MERGE INTO test2 dst USING test1 src ON dst.id = src.id WHEN MATCHED THEN UPDATE SET val = src.val");

        ResolvedDependencies deps = f.join();
        tester.checkDependencies(deps, t1Id);
        tester.checkDependencies(deps, t2Id);

        verify(registry, times(1)).getTable(anyInt(), eq(t1Id));
        verify(registry, times(1)).getTable(anyInt(), eq(t2Id));
    }

    /**
     * The same table should be requested only once per traversal.
     */
    @Test
    public void testCached() {
        IgniteTable table = createTestTable("TEST1");

        int t1Id = table.id();

        Tester tester = new Tester(createSchema(table));

        tester.setDependencies(t1Id, table1, update1);

        CompletableFuture<ResolvedDependencies> f = tester.resolveDependencies("SELECT (SELECT id FROM test1) FROM test1");

        ResolvedDependencies deps = f.join();
        tester.checkDependencies(deps, t1Id);

        verify(registry, times(1)).getTable(anyInt(), anyInt());
    }

    /**
     * Exception during dependency resolution is returned.
     */
    @Test
    public void testResolutionErrorIsReturned() {
        IgniteTable table = createTestTable("TEST1");

        int t1Id = table.id();

        Tester tester = new Tester(createSchema(table));

        RuntimeException err = new RuntimeException("Broken");
        tester.setError(t1Id, err);

        CompletableFuture<ResolvedDependencies> f = tester.resolveDependencies("SELECT * FROM test1");
        CompletionException wrapped = assertThrows(CompletionException.class, f::join);
        assertSame(err, wrapped.getCause());
    }

    private class Tester {

        final IgniteSchema igniteSchema;

        final Map<Integer, TestExecutableTable> deps = new HashMap<>();

        Tester(IgniteSchema schema) {
            igniteSchema = schema;
        }

        void setDependencies(int tableId, ScannableTable table, UpdatableTable updates) {
            TestExecutableTable executableTable = new TestExecutableTable(table, updates);

            deps.put(tableId, executableTable);

            CompletableFuture<ExecutableTable> f = CompletableFuture.completedFuture(executableTable);

            when(registry.getTable(anyInt(), eq(tableId))).thenReturn(f);
        }

        void setError(int tableId, Throwable err) {
            CompletableFuture<ExecutableTable> f = new CompletableFuture<>();
            f.completeExceptionally(err);

            when(registry.getTable(anyInt(), eq(tableId))).thenReturn(f);
        }

        CompletableFuture<ResolvedDependencies> resolveDependencies(String sql) {
            ExecutionDependencyResolver resolver = new ExecutionDependencyResolverImpl(registry, null);

            IgniteRel rel;
            try {
                rel = physicalPlan(sql, igniteSchema);
            } catch (Exception e) {
                throw new IllegalStateException("Unable to plan: " + sql, e);
            }

            return resolver.resolveDependencies(List.of(rel), igniteSchema.version());
        }

        void checkDependencies(ResolvedDependencies dependencies, int tableId) {
            TestExecutableTable executableTable = deps.get(tableId);

            assertEquals(executableTable.scannableTable(), dependencies.scannableTable(tableId));
            assertEquals(executableTable.updatableTable(), dependencies.updatableTable(tableId));
        }
    }

    private static final class TestExecutableTable implements ExecutableTable {

        private final ScannableTable table;

        private final UpdatableTable updates;

        TestExecutableTable(ScannableTable table, UpdatableTable updates) {
            this.table = table;
            this.updates = updates;
        }

        @Override
        public ScannableTable scannableTable() {
            return table;
        }

        @Override
        public UpdatableTable updatableTable() {
            return updates;
        }

        @Override
        public TableDescriptor tableDescriptor() {
            return updates.descriptor();
        }

        @Override
        public Supplier<PartitionCalculator> partitionCalculator() {
            return null;
        }
    }

    private static IgniteTable createTestTable(String tableName) {
        return createTestTable(tableName, null);
    }

    private static IgniteTable createTestTable(String tableName, @Nullable UnaryOperator<TableBuilder> changer) {
        TableBuilder testTable = TestBuilders.table()
                .name(tableName)
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .distribution(IgniteDistributions.single());

        if (changer != null) {
            changer.apply(testTable);
        }

        return testTable.build();
    }
}
