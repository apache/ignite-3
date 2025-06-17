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

package org.apache.ignite.internal.sql.engine.prepare.partitionawareness;

import java.util.List;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.jetbrains.annotations.Nullable;

/**
 * Extracts partition awareness metadata from physical plans. Examples:
 *
 * <pre>
 *     SELECT * FROM t WHERE pk=?
 *     colocation key: [pk]
 *     =>
 *     indexes: [1], hash: []
 *
 *     SELECT * FROM t WHERE pk1=? and pk2=?
 *     colocation key: [pk1, pk2]
 *     =>
 *     indexes: [1, 2], hash: []
 *
 *     SELECT * FROM t WHERE pk1=? and pk2=V1 and pk3=?
 *     colocation key: [pk1, pk2, pk3]
 *     =>
 *     indexes: [1, -1, 2], hash: [hash(V1)]
 * </pre>
 *
 * @see PartitionAwarenessMetadata
 */
public class PartitionAwarenessMetadataExtractor {

    /**
     * Extracts partition awareness metadata from the given IgniteKeyValueGet plan.
     *
     * @param kv IgniteKeyValueGet Plan.
     * @return Metadata.
     */
    @Nullable
    public static PartitionAwarenessMetadata getMetadata(IgniteKeyValueGet kv) {
        RelOptTable optTable = kv.getTable();
        assert optTable != null;

        List<RexNode> expressions = kv.keyExpressions();

        return buildMetadata(optTable, false, expressions);
    }

    /**
     * Extracts partition awareness metadata from the given IgniteKeyValueModify plan.
     *
     * @param kv IgniteKeyValueModify Plan.
     * @return Metadata.
     */
    @Nullable
    public static PartitionAwarenessMetadata getMetadata(IgniteKeyValueModify kv) {
        RelOptTable optTable = kv.getTable();
        assert optTable != null;

        List<RexNode> expressions = kv.expressions();

        return buildMetadata(optTable, true, expressions);
    }

    private static @Nullable PartitionAwarenessMetadata buildMetadata(
            RelOptTable optTable,
            boolean fullRow,
            List<RexNode> expressions
    ) {
        IgniteTable igniteTable = optTable.unwrap(IgniteTable.class);
        assert igniteTable != null;

        ImmutableIntList colocationKeys = igniteTable.distribution().getKeys();

        // colocation key index to dynamic param index
        int[] indexes = new int[colocationKeys.size()];
        int[] hash = new int[0];

        for (int i = 0; i < colocationKeys.size(); i++) {
            int colIdx = colocationKeys.get(i);
            RexNode expr;

            if (fullRow) {
                expr = expressions.get(colIdx);
            } else {
                int keyIdx = igniteTable.keyColumns().indexOf(colIdx);
                expr = expressions.get(keyIdx);
            }

            if (expr instanceof RexDynamicParam) {
                RexDynamicParam dynamicParam = (RexDynamicParam) expr;
                indexes[i] = dynamicParam.getIndex() + 1;
            } else {
                return null;
            }
        }

        return new PartitionAwarenessMetadata(igniteTable.id(), indexes, hash);
    }
}
