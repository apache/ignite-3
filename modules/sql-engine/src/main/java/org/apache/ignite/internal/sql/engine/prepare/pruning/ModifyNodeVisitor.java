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
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;
import org.apache.ignite.internal.sql.engine.rel.IgniteSelectCount;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteSetOp;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Visitor which traverse subtree below {@link IgniteTableModify modify node} in order to find all constant-at-runtime expressions to derive
 * metadata for partition pruning.
 */
class ModifyNodeVisitor implements IgniteRelVisitor<List<List<RexNode>>> {
    private static final IgniteLogger LOG = Loggers.forClass(ModifyNodeVisitor.class);

    /**
     * A placeholder used to designate that there is no value for a column. If PP metadata collection algorithm encounters this placeholder
     * in a colocation column for a source, then there can be no PP metadata for this source. Since this is a marker to terminate 
     * algorithm, the type chosen for this expression does not matter.
     */
    static final RexNode VALUE_NOT_ASSIGNED = new RexInputRef(99999, Commons.typeFactory().createSqlType(SqlTypeName.NULL));

    private final PartitionPruningMetadataExtractor extractor;

    private ModifyNodeVisitor(PartitionPruningMetadataExtractor extractor) {
        this.extractor = extractor;
    }

    /**
     * Returns list of expressions to derive pruning metadata from, or {@code null} if expressions can't be collected.
     *
     * <p>Every element from outer list contributes a partition, so after pruning all partitions defined by these
     * expressions must be preserved in the plan.
     *
     * @param extractor Metadata extractor to use.
     * @param tableModify A modify node to start traversal from.
     * @return A list of expressions. It's guaranteed that returned collection represents complete set of partitions. Returns {@code null}
     *         otherwise.
     */
    static @Nullable List<List<@Nullable RexNode>> go(
            PartitionPruningMetadataExtractor extractor,
            IgniteTableModify tableModify
    ) {
        switch (tableModify.getOperation()) {
            case INSERT:
            case UPDATE: {
                ModifyNodeVisitor visitor = new ModifyNodeVisitor(extractor);
                return visitor.visit((IgniteRel) tableModify.getInput());
            }
            case DELETE: {
                ModifyNodeVisitor visitor = new ModifyNodeVisitor(extractor);
                List<List<RexNode>> values = visitor.visit((IgniteRel) tableModify.getInput());

                if (nullOrEmpty(values)) {
                    return null;
                }

                IgniteTable table = tableModify.getTable().unwrap(IgniteTable.class);
                assert table != null;

                int numColumns = table.descriptor().columnsCount();
                ImmutableIntList keyColumns = table.keyColumns();
                Mapping mapping = Commons.projectedMapping(table.descriptor().columnsCount(), keyColumns);

                // Delete accept a key-only row, expand it to include all columns.
                List<List<RexNode>> fullRows = new ArrayList<>(values.size());

                for (List<RexNode> row : values) {
                    List<RexNode> fullRow = new ArrayList<>(numColumns);
                    for (int i = 0; i < numColumns; i++) {
                        fullRow.add(VALUE_NOT_ASSIGNED);
                    }

                    // Puts values of key columns into correct positions.
                    for (int k : keyColumns) {
                        int target = mapping.getTarget(k);
                        fullRow.set(k, row.get(target));
                    }

                    fullRows.add(fullRow);
                }

                return fullRows;
            }
            default:
                return null;
        }
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteValues rel) {
        return Commons.cast(rel.tuples);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteProject rel) {
        List<List<RexNode>> result = processSingleInputRel(rel);

        if (nullOrEmpty(result)) {
            return result;
        }

        List<List<RexNode>> projectedResult = new ArrayList<>(result.size());
        for (List<RexNode> inputExpressions : result) {
            RexVisitor<RexNode> inputInliner = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    return inputExpressions.get(inputRef.getIndex());
                }
            };

            projectedResult.add(inputInliner.visitList(rel.getProjects()));
        }

        return projectedResult;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteUnionAll rel) {
        List<IgniteRel> inputs = Commons.cast(rel.getInputs());
        List<List<RexNode>> results = new ArrayList<>();

        for (IgniteRel input : inputs) {
            List<List<RexNode>> inputResult = visit(input);
            if (inputResult == null) {
                return null;
            }

            results.addAll(inputResult);
        }

        return results;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSort rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteTableSpool rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSortedIndexSpool rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteLimit rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteHashIndexSpool rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSender rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteFilter rel) {
        if (rel.getCondition().isAlwaysFalse()) {
            return List.of();
        }

        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteTrimExchange rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteExchange rel) {
        return processSingleInputRel(rel);
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteNestedLoopJoin rel) {
        // processing of joins are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteHashJoin rel) {
        // processing of joins are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteCorrelatedNestedLoopJoin rel) {
        // processing of joins are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteMergeJoin rel) {
        // processing of joins are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteIndexScan rel) {
        extractor.visit(rel);

        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);
        assert table != null : "No table";

        return extractValuesFromScan(rel.sourceId(), table, rel.requiredColumns(), rel.projects());
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteTableScan rel) {
        extractor.visit(rel);

        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);
        assert table != null : "No table";

        return extractValuesFromScan(rel.sourceId(), table, rel.requiredColumns(), rel.projects());
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSystemViewScan rel) {
        // processing of system view scan is not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteReceiver rel) {
        // receiver is a leaf operator of fragment. It doesn't have information about remote input, 
        // thus pruning is impossible
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteColocatedHashAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteMapHashAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteReduceHashAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteColocatedSortAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteMapSortAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteReduceSortAggregate rel) {
        // processing of aggregates are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSetOp rel) {
        // processing of set operators are not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteTableFunctionScan rel) {
        // technically, we may try to forecast returned values by table functions, but this is not supported at the moment
        return null;
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteTableModify rel) {
        return processUnexpected("Plan can contains only one ModifyNode.");
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteKeyValueGet rel) {
        return processUnexpected("KV plans are not supposed to be part of distributed plans [node={}].", rel.getClass().getName());
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteKeyValueModify rel) {
        return processUnexpected("KV plans are not supposed to be part of distributed plans [node={}].", rel.getClass().getName());
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteSelectCount rel) {
        return processUnexpected("SelectCountPlan is not supposed to be part of distributed plans [node={}].", rel.getClass().getName());
    }

    @Override
    public @Nullable List<List<RexNode>> visit(IgniteRel rel) {
        return rel.accept(this);
    }

    private @Nullable List<List<RexNode>> processSingleInputRel(IgniteRel rel) {
        List<IgniteRel> inputs = Commons.cast(rel.getInputs());

        assert inputs.size() == 1 : "Relation with multiple inputs must be processed explicitly";

        //noinspection ConstantValue
        if (inputs.size() != 1) {
            return null;
        }

        return visit(inputs.get(0));
    }

    private static @Nullable List<List<RexNode>> processUnexpected(String messageTemplate, Object... args) {
        if (IgniteUtils.assertionsEnabled()) {
            String message = format(messageTemplate, args);

            assert false : message;
        }

        LOG.warn(messageTemplate, args);

        return null;
    }

    private @Nullable List<List<RexNode>> extractValuesFromScan(
            long sourceId,
            IgniteTable table,
            @Nullable ImmutableIntList requiredColumns,
            @Nullable List<RexNode> projects
    ) {
        PartitionPruningColumns metadata = extractor.result.get(sourceId);
        if (metadata == null) {
            return null;
        }

        List<List<RexNode>> values = metadataToValues(table, metadata);

        // Apply projection formed by requiredColumns if any.
        if (requiredColumns != null) {
            List<List<RexNode>> projectedValues = new ArrayList<>(values.size());

            for (List<RexNode> row : values) {
                List<RexNode> projectedRow = new ArrayList<>(requiredColumns.size());

                for (int i = 0; i < requiredColumns.size(); i++) {
                    projectedRow.add(row.get(requiredColumns.getInt(i)));
                }

                projectedValues.add(projectedRow);
            }

            values = projectedValues;
        }

        // Apply projection attached to a scan if any.
        if (!nullOrEmpty(projects)) {
            List<List<RexNode>> projectedValues = new ArrayList<>(values.size());

            for (List<RexNode> inputRow : values) {
                RexVisitor<RexNode> inputInliner = new RexShuttle() {
                    @Override
                    public RexNode visitLocalRef(RexLocalRef localRef) {
                        return inputRow.get(localRef.getIndex());
                    }
                };
                projectedValues.add(inputInliner.visitList(projects));
            }

            values = projectedValues;
        }

        return values;
    }

    private static List<List<RexNode>> metadataToValues(
            IgniteTable table,
            PartitionPruningColumns metadata
    ) {
        // Creates a row for each PP metadata entry. Consider the following example
        // of a scan predicate that includes colocation columns C1, C2, C3:
        //
        // c1 IN (10, ?0, 42) AND c2 = ?1 AND c3 = 99
        //
        // It's metadata is:
        //  [c1=10, c2=?1, c3=99]
        //  [c1=?0, c2=?1, c3=99]
        //  [c1=42, c2=?1, c3=99]
        //
        // That metadata is equivalent to 3 rows:
        //  [10, ?1, 99]
        //  [?0, ?1, 99]
        //  [42, ?1, 99]
        //

        int numColumns = table.descriptor().columnsCount();
        List<List<RexNode>> projectionAsValues = new ArrayList<>(metadata.columns().size());

        for (Int2ObjectMap<RexNode> columns : metadata.columns()) {
            List<RexNode> row = new ArrayList<>(numColumns);

            // There are more non-colocation key columns than colocation key ones.
            // So init all columns with unknown-value placeholders first. 
            for (int i = 0; i < numColumns; i++) {
                row.add(VALUE_NOT_ASSIGNED);
            }

            // Then set colocation key columns. 
            for (Int2ObjectMap.Entry<RexNode> entry : columns.int2ObjectEntrySet()) {
                int col = entry.getIntKey();
                row.set(col, entry.getValue());
            }

            projectionAsValues.add(row);
        }

        return projectionAsValues;
    }
}
