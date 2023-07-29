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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
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
        Tester tester = new Tester();

        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("ID", SqlTypeName.INTEGER)
                .build();

        int t1Id = tester.addTable("TEST1", tableType);
        tester.setDependencies(t1Id, table1, update1);

        int t2Id = tester.addTable("TEST2", tableType);
        tester.setDependencies(t2Id, table2, update2);

        CompletableFuture<ResolvedDependencies> f = tester.resolveDependencies("SELECT * FROM test1 JOIN test2 ON test1.id = test2.id");

        ResolvedDependencies deps = f.join();
        tester.checkDependencies(deps, t1Id);
        tester.checkDependencies(deps, t2Id);

        TableDescriptor td1 = tester.tableDescriptor("TEST1");
        TableDescriptor td2 = tester.tableDescriptor("TEST2");

        verify(registry, times(1)).getTable(eq(t1Id), same(td1));
        verify(registry, times(1)).getTable(eq(t2Id), same(td2));
    }

    /**
     * Index scan.
     */
    @Test
    public void testIndexScan() {
        Tester tester = new Tester();

        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("ID", SqlTypeName.INTEGER)
                .build();

        int t1Id = tester.addTable("TEST1", tableType);
        tester.setDependencies(t1Id, table1, update1);
        tester.addIndex("TEST1", new IgniteIndex(TestHashIndex.create(List.of("ID"), "ID_IDX")));

        CompletableFuture<ResolvedDependencies> f = tester.resolveDependencies("SELECT * FROM test1 WHERE id=1");
        tester.checkDependencies(f.join(), t1Id);

        TableDescriptor td1 = tester.tableDescriptor("TEST1");

        verify(registry, times(1)).getTable(eq(t1Id), same(td1));
    }

    /**
     * DML.
     */
    @Test
    public void testModify() {
        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("ID", SqlTypeName.INTEGER)
                .add("VAL", SqlTypeName.INTEGER)
                .build();

        Tester tester = new Tester();

        int t1Id = tester.addTable("TEST1", tableType);
        tester.setDependencies(t1Id, table1, update1);

        int t2Id = tester.addTable("TEST2", tableType);
        tester.setDependencies(t2Id, table2, update2);

        CompletableFuture<ResolvedDependencies> f = tester.resolveDependencies(
                "MERGE INTO test2 dst USING test1 src ON dst.id = src.id WHEN MATCHED THEN UPDATE SET val = src.val");

        ResolvedDependencies deps = f.join();
        tester.checkDependencies(deps, t1Id);
        tester.checkDependencies(deps, t2Id);

        TableDescriptor td1 = tester.tableDescriptor("TEST1");
        TableDescriptor td2 = tester.tableDescriptor("TEST2");

        verify(registry, times(1)).getTable(eq(t1Id), same(td1));
        verify(registry, times(1)).getTable(eq(t2Id), same(td2));
    }

    /**
     * The same table should be requested only once per traversal.
     */
    @Test
    public void testCached() {
        Tester tester = new Tester();

        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("ID", SqlTypeName.INTEGER)
                .build();

        String tableName = "TEST1";
        int t1Id = tester.addTable(tableName, tableType);
        tester.setDependencies(t1Id, table1, update1);

        CompletableFuture<ResolvedDependencies> f = tester.resolveDependencies("SELECT (SELECT id FROM test1) FROM test1");

        ResolvedDependencies deps = f.join();
        tester.checkDependencies(deps, t1Id);

        verify(registry, times(1)).getTable(anyInt(), any(TableDescriptor.class));
    }

    /**
     * Exception during dependency resolution is returned.
     */
    @Test
    public void testResolutionErrorIsReturned() {
        Tester tester = new Tester();

        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("ID", SqlTypeName.INTEGER)
                .build();

        int t1Id = tester.addTable("TEST1", tableType);

        RuntimeException err = new RuntimeException("Broken");
        tester.setError(t1Id, err);

        CompletableFuture<ResolvedDependencies> f = tester.resolveDependencies("SELECT * FROM test1");
        CompletionException wrapped = assertThrows(CompletionException.class, f::join);
        assertSame(err, wrapped.getCause());
    }

    /** Fetch colocation group succeeds. */
    @Test
    public void testFetchColocationGroup() {
        Tester tester = new Tester();

        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("ID", SqlTypeName.INTEGER)
                .build();

        int t1Id = tester.addTable("TEST1", tableType);
        ColocationGroup g1 = ColocationGroup.forNodes(List.of("n1"));

        tester.setColocationGroup(t1Id, CompletableFuture.completedFuture(g1));
        tester.setDependencies(t1Id, table1, update1);

        ResolvedDependencies deps = tester.resolveDependencies("SELECT * FROM test1").join();

        ColocationGroup actual = deps.fetchColocationGroup(t1Id).join();
        assertSame(g1, actual);
    }

    /** Fetch colocation group propagates an error. */
    @Test
    public void testFetchColocationGroupErrorIsReturned() {
        Tester tester = new Tester();

        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("ID", SqlTypeName.INTEGER)
                .build();

        int t1Id = tester.addTable("TEST1", tableType);

        ClusterNode node1 = new ClusterNodeImpl("1", "node1", new NetworkAddress("host", 1234));

        List<PrimaryReplica> rs = new ArrayList<>();
        rs.add(new PrimaryReplica(node1, 5));

        IllegalStateException error = new IllegalStateException("Not available");
        tester.setColocationGroup(t1Id, CompletableFuture.failedFuture(error));
        tester.setDependencies(t1Id, table1, update1);

        ResolvedDependencies deps = tester.resolveDependencies("SELECT * FROM test1").join();

        CompletionException err = assertThrows(CompletionException.class, () -> deps.fetchColocationGroup(t1Id).join());
        assertSame(error, err.getCause());
    }

    private class Tester {

        final IgniteSchema igniteSchema = new IgniteSchema("PUBLIC");

        final Map<Integer, TestExecutableTable> deps = new HashMap<>();

        final Map<Integer, CompletableFuture<ColocationGroup>> colocationGroups = new HashMap<>();

        int addTable(String name, RelDataType rowType) {
            IgniteTable table = createTable(igniteSchema, name, rowType, IgniteDistributions.single());
            igniteSchema.addTable(table);

            return table.id();
        }

        void addIndex(String tableName, IgniteIndex index) {
            IgniteTable table = (IgniteTable) igniteSchema.getTable(tableName);
            Objects.requireNonNull(table, "No table");
            table.addIndex(index);
        }

        void setDependencies(int tableId, ScannableTable table, UpdatableTable updates) {
            CompletableFuture<ColocationGroup> defaultReplicaList = CompletableFuture.failedFuture(new IllegalStateException());
            CompletableFuture<ColocationGroup> replicaList = colocationGroups.getOrDefault(tableId, defaultReplicaList);

            TestExecutableTable executableTable = new TestExecutableTable(table, updates, replicaList);

            deps.put(tableId, executableTable);

            CompletableFuture<ExecutableTable> f = CompletableFuture.completedFuture(executableTable);

            when(registry.getTable(eq(tableId), any(TableDescriptor.class))).thenReturn(f);
        }

        void setError(int tableId, Throwable err) {
            CompletableFuture<ExecutableTable> f = new CompletableFuture<>();
            f.completeExceptionally(err);

            when(registry.getTable(eq(tableId), any(TableDescriptor.class))).thenReturn(f);
        }

        void setColocationGroup(int tableId, CompletableFuture<ColocationGroup> group) {
            colocationGroups.put(tableId, group);
        }

        CompletableFuture<ResolvedDependencies> resolveDependencies(String sql) {
            ExecutionDependencyResolver resolver = new ExecutionDependencyResolverImpl(registry);

            IgniteRel rel;
            try {
                rel = physicalPlan(sql, igniteSchema);
            } catch (Exception e) {
                throw new IllegalStateException("Unable to plan: " + sql, e);
            }

            return resolver.resolveDependencies(List.of(rel), 1);
        }

        void checkDependencies(ResolvedDependencies dependencies, int tableId) {
            TestExecutableTable executableTable = deps.get(tableId);

            assertEquals(executableTable.scannableTable(), dependencies.scannableTable(tableId));
            assertEquals(executableTable.updatableTable(), dependencies.updatableTable(tableId));
        }

        TableDescriptor tableDescriptor(String tableName) {
            IgniteTable table = (IgniteTable) igniteSchema.getTable(tableName);
            return table.descriptor();
        }
    }

    private static final class TestExecutableTable implements ExecutableTable {

        private final ScannableTable table;

        private final UpdatableTable updates;

        private final CompletableFuture<ColocationGroup> colocationGroup;

        TestExecutableTable(ScannableTable table, UpdatableTable updates, CompletableFuture<ColocationGroup> colocationGroup) {
            this.table = table;
            this.updates = updates;
            this.colocationGroup = colocationGroup;
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
        public CompletableFuture<ColocationGroup> fetchColocationGroup() {
            return colocationGroup;
        }
    }
}
