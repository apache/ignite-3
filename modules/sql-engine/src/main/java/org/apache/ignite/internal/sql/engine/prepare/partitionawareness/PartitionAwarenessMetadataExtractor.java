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

import static org.apache.ignite.internal.sql.engine.util.RexUtils.getValueFromLiteral;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify.Operation;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.ColocationUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Extracts partition awareness metadata from physical plans. Examples:
 *
 * <pre>
 *     SELECT * FROM t WHERE pk=?
 *     colocation key: [pk]
 *     =>
 *     indexes: [0], hash: []
 *
 *     SELECT * FROM t WHERE pk1=? and pk2=?
 *     colocation key: [pk1, pk2]
 *     =>
 *     indexes: [0, 1], hash: []
 *
 *     SELECT * FROM t WHERE pk1=? and pk2=V1 and pk3=?
 *     colocation key: [pk1, pk2, pk3]
 *     =>
 *     indexes: [0, -1, 1], hash: [hash(V1)]
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

        return buildMetadata(optTable, false, expressions, DirectTxMode.SUPPORTED);
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

        return buildMetadata(optTable, kv.operation() == Operation.INSERT, expressions, DirectTxMode.SUPPORTED_TRACKING_REQUIRED);
    }

    private static @Nullable PartitionAwarenessMetadata buildMetadata(
            RelOptTable optTable,
            boolean fullRow,
            List<RexNode> expressions,
            DirectTxMode directTxMode
    ) {
        IgniteTable igniteTable = optTable.unwrap(IgniteTable.class);
        assert igniteTable != null;

        ImmutableIntList colocationKeys = igniteTable.distribution().getKeys();

        // colocation key index to dynamic param index
        int[] indexes = new int[colocationKeys.size()];
        IntArrayList hashFields = new IntArrayList(colocationKeys.size() / 2);

        boolean onlyLiterals = true;
        int hashPos = -1;

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
                indexes[i] = dynamicParam.getIndex();
                onlyLiterals = false;
            } else if (expr instanceof RexLiteral) {
                RexLiteral expr0 = (RexLiteral) expr;

                // depends on supplied zoneId, it can`t be cached
                if (expr0.getTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                    return null;
                }

                indexes[i] = hashPos--;
                NativeType nativeType = IgniteTypeFactory.relDataTypeToNative(expr0.getType());
                Object val = getValueFromLiteral(nativeType, expr0);
                hashFields.add(ColocationUtils.hash(val, nativeType));
            } else {
                return null;
            }
        }

        // case for IgniteKeyValueModify with only literals in colocation columns.
        if (fullRow && onlyLiterals) {
            return null;
        }

        int[] hash = hashFields.toArray(new int[0]);

        return new PartitionAwarenessMetadata(igniteTable.id(), indexes, hash, directTxMode);
    }
}
