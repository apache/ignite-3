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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import java.util.List;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.prepare.RelWithSources;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningColumns;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify.Operation;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.Primitives;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.sql.engine.util.RexUtils.FaultyContext;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.ColocationUtils;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.QualifiedNameHelper;
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
     * Extracts partition awareness metadata from the given plan.
     *
     * @param relationWithSources Relation with sources.
     * @param partitionPruningMetadata Partition-pruning metadata.
     * @return Metadata.
     */
    public static @Nullable PartitionAwarenessMetadata getMetadata(
            RelWithSources relationWithSources,
            @Nullable PartitionPruningMetadata partitionPruningMetadata
    ) {
        IgniteRel rel = relationWithSources.root();

        if (rel instanceof IgniteKeyValueGet) {
            return getMetadata((IgniteKeyValueGet) rel);
        } else if (rel instanceof IgniteKeyValueModify) {
            return getMetadata((IgniteKeyValueModify) rel);
        } else if (partitionPruningMetadata != null) {
            return tryConvertPartitionPruningMetadata(relationWithSources, partitionPruningMetadata);
        } else {
            return null;
        }
    }

    /**
     * Extracts partition awareness metadata from the given IgniteKeyValueGet plan.
     *
     * @param kv IgniteKeyValueGet Plan.
     * @return Metadata.
     */
    @Nullable
    private static PartitionAwarenessMetadata getMetadata(IgniteKeyValueGet kv) {
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
    private static PartitionAwarenessMetadata getMetadata(IgniteKeyValueModify kv) {
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

        for (int i = 0; i < colocationKeys.size(); i++) {
            int colIdx = colocationKeys.get(i);
            RexNode expr;

            if (fullRow) {
                expr = expressions.get(colIdx);
            } else {
                int keyIdx = igniteTable.keyColumns().indexOf(colIdx);
                expr = expressions.get(keyIdx);
            }

            boolean added = addToMetadata(expr, indexes, i, hashFields);
            if (!added) {
                return null;
            }
        }

        int[] hash = hashFields.toIntArray();

        return new PartitionAwarenessMetadata(igniteTable.id(), indexes, hash, directTxMode, qualifiedName(optTable));
    }

    private static QualifiedName qualifiedName(RelOptTable optTable) {
        List<String> nameParts = optTable.getQualifiedName();
        assert nameParts.size() >= 2 : "Invalid qualified name: " + nameParts;

        return QualifiedNameHelper.fromNormalized(nameParts.get(0), nameParts.get(1));
    }

    private static @Nullable PartitionAwarenessMetadata tryConvertPartitionPruningMetadata(
            RelWithSources relationWithSources,
            PartitionPruningMetadata metadata
    ) {
        // Partition awareness metadata is created once per source table,
        // so we do not consider plan that have more then 1 source.
        if (metadata.data().size() != 1) {
            return null;
        }

        Long2ObjectMap.Entry<PartitionPruningColumns> entry = metadata.data()
                .long2ObjectEntrySet()
                .iterator().next();

        long sourceId = entry.getLongKey();
        IgniteRel sourceRel = relationWithSources.get(sourceId);
        assert sourceRel != null;

        RelOptTable optTable = sourceRel.getTable();
        assert optTable != null;

        IgniteTable igniteTable = optTable.unwrap(IgniteTable.class);
        assert igniteTable != null;

        // Partition pruning (PP) metadata includes information to identify all possible partitions. 
        // However, partition awareness restricts execution to a single partition, 
        // so we should reject PP metadata that has more than one set of columns.
        //
        // Ignore PP with correlated variables as well, because some queries 
        // can access additional partitions.
        PartitionPruningColumns columns = entry.getValue();
        if (columns.columns().size() != 1 || columns.containCorrelatedVariables()) {
            return null;
        }

        boolean dml = relationWithSources.modifiedTables().contains(igniteTable.id());
        long numSources = numberOfModifyAndSourceRels(relationWithSources);

        // Accept queries that have exactly one source rel.
        if (!dml && numSources != 1) {
            return null;
        }

        // Accept DMLs that have a ModifyNode and a single source rel.
        if (dml && numSources != 2) {
            return null;
        }

        // Choose appropriate tx mode.
        DirectTxMode directTxMode = dml ? DirectTxMode.NOT_SUPPORTED : DirectTxMode.SUPPORTED;

        ImmutableIntList colocationKeys = igniteTable.distribution().getKeys();
        int[] indexes = new int[colocationKeys.size()];
        IntArrayList hashFields = new IntArrayList(colocationKeys.size());
        Int2ObjectMap<RexNode> cols = columns.columns().get(0);

        for (Int2ObjectMap.Entry<RexNode> colEntry : cols.int2ObjectEntrySet()) {
            RexNode colExpr = colEntry.getValue();
            int colIdx = colEntry.getIntKey();
            int i = colocationKeys.indexOf(colIdx);
            assert i >= 0 : "Invalid colocation column index: " + cols.keySet();

            boolean added = addToMetadata(colExpr, indexes, i, hashFields);
            if (!added) {
                return null;
            }
        }

        int[] hash = hashFields.toIntArray();

        return new PartitionAwarenessMetadata(igniteTable.id(), indexes, hash, directTxMode, qualifiedName(optTable));
    }

    private static long numberOfModifyAndSourceRels(RelWithSources relationWithSources) {
        Long2ObjectMap<IgniteRel> sources = relationWithSources.sources();
        // When counting the number of source relations ignore safe table functions as they these function 
        // produce the same result and do not affect / are not affected by data distribution.
        return sources.values().stream()
                .filter(r -> {
                    if (!(r instanceof IgniteTableFunctionScan)) {
                        return true;
                    } else {
                        // Only allow to use the SYSTEM_RANGE table function,
                        // since that function always produces the same results
                        // and does not require any external dependencies to run.
                        IgniteTableFunctionScan scan = (IgniteTableFunctionScan) r;
                        return !RexUtil.isCallTo(scan.getCall(), IgniteSqlOperatorTable.SYSTEM_RANGE);
                    }
                })
                .count();
    }

    private static boolean addToMetadata(RexNode colExpr, int[] indexes, int i, IntArrayList hashFields) {
        if (colExpr instanceof RexDynamicParam) {
            RexDynamicParam dynamicParam = (RexDynamicParam) colExpr;
            indexes[i] = dynamicParam.getIndex();

            return true;
        } else if (colExpr instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) colExpr;

            // depends on supplied zoneId, it can`t be cached
            if (literal.getTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                return false;
            }

            int hashValue = computeHash(literal);
            hashFields.add(hashValue);
            indexes[i] = -hashFields.size();

            return true;
        } else {
            return false;
        }
    }

    private static int computeHash(RexLiteral literal) {
        Class<?> internalType = Primitives.wrap((Class<?>) Commons.typeFactory().getJavaClass(literal.getType()));
        Object val = RexUtils.literalValue(FaultyContext.INSTANCE, literal, internalType);

        NativeType nativeType = IgniteTypeFactory.relDataTypeToNative(literal.getType());
        Object internalVal = TypeUtils.fromInternal(val, nativeType.spec());

        return ColocationUtils.hash(internalVal, nativeType);
    }
}
