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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningColumns;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Tests for {@link PartitionProvider} implementations. */
@ExtendWith(MockitoExtension.class)
public class PartitionProvidersTest extends BaseIgniteAbstractTest {

    private static final long GROUP_ID = 1;

    @Mock
    private QueryTaskExecutor queryTaskExecutor;

    @Test
    public void testStaticPartitionProviderWithProvidedPartitions() {
        Int2ObjectMap<NodeWithConsistencyToken> assignments = new Int2ObjectOpenHashMap<>();
        assignments.put(0, new NodeWithConsistencyToken("n1", 0));
        assignments.put(1, new NodeWithConsistencyToken("n2", 0));
        assignments.put(2, new NodeWithConsistencyToken("n3", 0));

        Map<String, List<PartitionWithConsistencyToken>> partitionsPerNode = Map.of(
                "n1", List.of(new PartitionWithConsistencyToken(1, 1L)),
                "n2", List.of(new PartitionWithConsistencyToken(2, 2L)),
                "n3", List.of(new PartitionWithConsistencyToken(3, 3L))
        );

        IgniteTable table = TestBuilders.table()
                .name("T1")
                .addKeyColumn("C1", NativeTypes.INT32)
                .partitions(42)
                .distribution(IgniteDistributions.affinity(List.of(0), 2, "3"))
                .partitions(assignments.size())
                .build();

        ColocationGroup group = new ColocationGroup(
                List.of(1L, 2L), List.of("n1", "n2", "n3"),
                assignments,
                partitionsPerNode
        );

        {
            ExecutionContext<Object[]> ctx = newContext("n1", group, null);
            expectPartitions(ctx, table, List.of(
                    new PartitionWithConsistencyToken(1, 1L)
            ));
        }

        {
            ExecutionContext<Object[]> ctx = newContext("n2", group, null);
            expectPartitions(ctx, table, List.of(
                    new PartitionWithConsistencyToken(2, 2L)
            ));
        }

        {
            ExecutionContext<Object[]> ctx = newContext("n3", group, null);
            expectPartitions(ctx, table, List.of(
                    new PartitionWithConsistencyToken(3, 3L)
            ));
        }
    }

    @Test
    public void testStaticPartitionProviderUsesAssignmentsWhenNoPartitionsPresent() {
        Int2ObjectMap<NodeWithConsistencyToken> assignments = new Int2ObjectOpenHashMap<>();
        assignments.put(0, new NodeWithConsistencyToken("n1", 0));
        assignments.put(1, new NodeWithConsistencyToken("n2", 1));
        assignments.put(2, new NodeWithConsistencyToken("n1", 2));
        assignments.put(3, new NodeWithConsistencyToken("n1", 3));
        assignments.put(4, new NodeWithConsistencyToken("n3", 4));

        IgniteTable table = TestBuilders.table()
                .name("T1")
                .addKeyColumn("C1", NativeTypes.INT32)
                .partitions(assignments.size())
                .distribution(IgniteDistributions.affinity(List.of(0), 2, "3"))
                .partitions(assignments.size())
                .build();

        ColocationGroup group = new ColocationGroup(
                List.of(1L, 2L), List.of("n1", "n2", "n3"),
                assignments
        );

        {
            ExecutionContext<Object[]> ctx = newContext("n1", group, null);
            expectPartitions(ctx, table,
                    List.of(
                            new PartitionWithConsistencyToken(0, 0L),
                            new PartitionWithConsistencyToken(2, 2L),
                            new PartitionWithConsistencyToken(3, 3L)
                    )
            );
        }

        {
            ExecutionContext<Object[]> ctx = newContext("n2", group, null);
            expectPartitions(ctx, table, List.of(new PartitionWithConsistencyToken(1, 1L)));
        }

        {
            ExecutionContext<Object[]> ctx = newContext("n3", group, null);
            expectPartitions(ctx, table, List.of(new PartitionWithConsistencyToken(4, 4L)));
        }
    }

    @Test
    public void testDynamicPartitionProvider() {
        Int2ObjectMap<NodeWithConsistencyToken> assignments = new Int2ObjectOpenHashMap<>();
        assignments.put(0, new NodeWithConsistencyToken("n1", 0));
        assignments.put(1, new NodeWithConsistencyToken("n2", 1));
        assignments.put(2, new NodeWithConsistencyToken("n1", 2));
        assignments.put(3, new NodeWithConsistencyToken("n1", 3));
        assignments.put(4, new NodeWithConsistencyToken("n3", 4));

        IgniteTable table = TestBuilders.table()
                .name("T1")
                .addKeyColumn("C1", NativeTypes.INT32)
                .partitions(assignments.size())
                .distribution(IgniteDistributions.affinity(List.of(0), 2, "3"))
                .partitions(assignments.size())
                .build();

        ColocationGroup group = new ColocationGroup(
                List.of(1L, 2L), List.of("n1", "n2", "n3"),
                assignments,
                Map.of()
        );

        PartitionPruningMetadata metadata = newMetadata(1);

        {
            ExecutionContext<Object[]> ctx = newContext("n1", group, metadata);
            expectPartitions(ctx, table, List.of());
        }

        {
            ExecutionContext<Object[]> ctx = newContext("n2", group, metadata);
            expectPartitions(ctx, table, List.of(new PartitionWithConsistencyToken(1, 1L)));
        }

        {
            ExecutionContext<Object[]> ctx = newContext("n3", group, metadata);
            expectPartitions(ctx, table, List.of());
        }
    }

    private ExecutionContext<Object[]> newContext(
            String nodeName,
            ColocationGroup colocationGroup,
            @Nullable PartitionPruningMetadata metadata
    ) {
        ClusterNodeImpl node = new ClusterNodeImpl(nodeName, nodeName, new NetworkAddress("localhost", 1234));

        Long2ObjectMap<ColocationGroup> map = new Long2ObjectOpenHashMap<>();
        map.put(GROUP_ID, colocationGroup);

        return TestBuilders.executionContext()
                .queryId(UUID.randomUUID())
                .executor(queryTaskExecutor)
                .fragment(new FragmentDescription(1, false, map, null, null, metadata))
                .localNode(node)
                .build();
    }

    private static PartitionPruningMetadata newMetadata(long sourceId) {
        RexNode expr = Commons.rexBuilder().makeLiteral(1, Commons.typeFactory().createSqlType(SqlTypeName.INTEGER));
        PartitionPruningColumns columns = new PartitionPruningColumns(List.of(Int2ObjectMaps.singleton(0, expr)));

        return new PartitionPruningMetadata(Long2ObjectMaps.singleton(sourceId, columns));
    }

    private static void expectPartitions(
            ExecutionContext<Object[]> ctx,
            IgniteTable table,
            List<PartitionWithConsistencyToken> expected
    ) {
        ColocationGroup group = ctx.group(GROUP_ID);
        assertNotNull(group, "no group does not exist: " + GROUP_ID);

        PartitionProvider<Object[]> partitionProvider = ctx.getPartitionProvider(1, group, table);

        List<PartitionWithConsistencyToken> actual = partitionProvider.getPartitions(ctx);
        assertEquals(expected, actual, "Node: " + ctx.localNode().name());
    }
}
