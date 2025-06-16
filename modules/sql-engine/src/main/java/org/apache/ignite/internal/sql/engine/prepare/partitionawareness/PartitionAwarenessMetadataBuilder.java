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

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;

/**
 * Extracts partition awareness metadata from physical plans.
 */
public class PartitionAwarenessMetadataBuilder {

    /**
     * Extracts partition awareness metadata from the given physical plan.
     *
     * @param rel Physical plan.
     * @return Metadata.
     */
    @Nullable
    public static PartitionAwarenessMetadata build(IgniteRel rel) {
        if (rel instanceof IgniteKeyValueGet) {
            IgniteKeyValueGet kv = (IgniteKeyValueGet) rel;
            RelOptTable optTable = rel.getTable();
            assert optTable != null;

            return extractMetadata(optTable, kv.keyExpressions());
        } else if (rel instanceof IgniteKeyValueModify) {
            IgniteKeyValueModify kv = (IgniteKeyValueModify) rel;

            RelOptTable optTable = rel.getTable();
            assert optTable != null;

            return extractMetadata(optTable, kv.expressions());
        } else {
            return null;
        }
    }

    @Nullable
    private static PartitionAwarenessMetadata extractMetadata(
            RelOptTable optTable,
            List<RexNode> expressions
    ) {
        IgniteTable igniteTable = optTable.unwrap(IgniteTable.class);
        assert igniteTable != null;

        ImmutableIntList colocationKeys = igniteTable.distribution().getKeys();

        // colocation key index to dynamic param index
        int[] dynamicParams = new int[colocationKeys.size()];
        Arrays.fill(dynamicParams, -1);

        int[] hash = new int[0];

        for (int i = 0; i < colocationKeys.size(); i++) {
            int colIdx = colocationKeys.get(i);
            int keyIdx = igniteTable.keyColumns().indexOf(colIdx);
            RexNode expr = expressions.get(keyIdx);

            if (expr instanceof RexDynamicParam) {
                RexDynamicParam dynamicParam = (RexDynamicParam) expr;
                dynamicParams[i] = dynamicParam.getIndex();
            } else {
                return null;
            }
        }

        return new PartitionAwarenessMetadata(igniteTable.id(), dynamicParams, hash);
    }
}
