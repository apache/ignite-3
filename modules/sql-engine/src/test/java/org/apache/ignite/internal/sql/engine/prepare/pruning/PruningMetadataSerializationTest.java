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

package org.apache.ignite.internal.sql.engine.prepare.pruning;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests serialization of {@link PartitionPruningMetadata}.
 */
public class PruningMetadataSerializationTest extends BaseIgniteAbstractTest {

    private final ClassDescriptorRegistry descriptorRegistry = new ClassDescriptorRegistry();

    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    @Test
    public void test() throws Exception {

        RexBuilder rexBuilder = Commons.rexBuilder();
        RexNode lit1 = rexBuilder.makeLiteral("1");
        RexNode lit2 = rexBuilder.makeLiteral(true);

        Long2ObjectMap<PartitionPruningColumns> map = new Long2ObjectOpenHashMap<>();

        map.put(1, new PartitionPruningColumns(List.of()));
        map.put(2, new PartitionPruningColumns(List.of(
                Int2ObjectMaps.singleton(0, lit1)
        )));

        Int2ObjectMap<RexNode> cols = new Int2ObjectOpenHashMap<>();
        cols.put(0, lit2);
        cols.put(1, lit1);
        map.put(3, new PartitionPruningColumns(List.of(cols)));

        PartitionPruningMetadata metadata = new PartitionPruningMetadata(map);

        byte[] bytes = marshaller.marshal(metadata).bytes();

        PartitionPruningMetadata fromBytes = marshaller.unmarshal(bytes, descriptorRegistry);
        assertNotNull(fromBytes, "Deserialized to null");

        expectColumns(fromBytes, 1, List.of());
        expectColumns(fromBytes, 2, List.of(Map.of(0, lit1)));
        expectColumns(fromBytes, 3, List.of(Map.of(0, lit2, 1, lit1)));
    }

    private static void expectColumns(
            PartitionPruningMetadata metadata,
            long sourceId,
            List<Map<Integer, RexNode>> cols
    ) {
        PartitionPruningColumns columns = metadata.get(sourceId);
        assertNotNull(columns, format("No metadata for source#{}: {}", sourceId, metadata));

        assertEquals(cols, columns.columns(), format("Metadata for source#{} does not match", sourceId));
    }
}
