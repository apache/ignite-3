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

package org.apache.ignite.internal.sql.engine.metadata;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.jetbrains.annotations.Nullable;

/**
 * IgniteMdRowCount.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdRowCount extends RelMdRowCount {
    public static final double NON_EQUI_COEFF = 0.7;

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.ROW_COUNT.method, new IgniteMdRowCount());

    /** {@inheritDoc} */
    @Override
    public @Nullable Double getRowCount(Join rel, RelMetadataQuery mq) {
        return joinRowCount(mq, rel);
    }

    /** {@inheritDoc} */
    @Override
    public Double getRowCount(Sort rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /**
     * RowCount of Spool equals to estimated row count of its child by default, but IndexSpool has internal filter that
     * could filter out some rows, hence we need to estimate it differently.
     */
    public double getRowCount(IgniteSortedIndexSpool rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /**
     * Estimation of row count for Intersect operator.
     */
    @Override public Double getRowCount(Intersect rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /**
     * Estimation of row count for Minus operator.
     */
    @Override public Double getRowCount(Minus rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /**
     * Estimation of row count for Aggregate operator.
     */
    public double getRowCount(IgniteAggregate rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /**
     * Estimation of row count for Limit operator.
     */
    public double getRowCount(IgniteLimit rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /**
     * Estimates the number of rows produced by a join operation.
     *
     * <p>This method calculates an estimated row count for a join by analyzing the join type,
     * join keys, and the cardinality of the left and right inputs. It provides specialized
     * handling for primary key and foreign key relationships (see {@link JoiningRelationType for details}).
     * When certain metadata is unavailable or when specific conditions are not met, it falls back to
     * Calcite's default implementation for estimating the row count.
     *
     * <p>Implementation details:</p>
     * <ul>
     *   <li>If the join type is not {@link JoinRelType#INNER}, Calcite's default implementation is used.</li>
     *   <li>If the join is non-equi join, Calcite's default implementation is used.</li>
     *   <li>The row counts of the left and right inputs are retrieved using 
     *   {@link RelMetadataQuery#getRowCount}. If either value is unavailable, the result is {@code null}.</li>
     *   <li>If the row counts are very small (≤ 1.0), the method uses the maximum row count as a fallback.</li>
     *   <li>Join key origins are resolved for the left and right inputs, and relationships between tables 
     *   (e.g., primary key or foreign key associations) are identified and grouped into join contexts.</li>
     *   <li>If no valid join context is found, the method falls back to Calcite's implementation.</li>
     *   <li>The base row count is determined by the type of join relationship:
     *       <ul>
     *           <li>For primary key-to-primary key joins, the row count is based on the smaller table, 
     *           adjusted by a percentage of the larger table's rows.</li>
     *           <li>For foreign key joins, the base table is determined based on which table is 
     *           joined using non-primary key columns.</li>
     *       </ul>
     *   </li>
     *   <li>An additional adjustment factor is applied for post-filtration conditions, such as extra join keys 
     *   or non-equi conditions.</li>
     *   <li>If metadata for the percentage of original rows is unavailable, the adjustment defaults to 1.0.</li>
     * </ul>
     *
     * <p>If none of the above criteria are satisfied, the method defaults to 
     * {@link RelMdUtil#getJoinRowCount} for the estimation.</p>
     *
     * @param mq The {@link RelMetadataQuery} used to retrieve metadata about relational expressions.
     * @param rel The {@link Join} relational expression representing the join operation.
     * @return The estimated number of rows resulting from the join, or {@code null} if the estimation cannot be determined.
     *
     * @see RelMetadataQuery#getRowCount
     * @see RelMdUtil#getJoinRowCount
     * @see JoiningRelationType
     */
    public static @Nullable Double joinRowCount(RelMetadataQuery mq, Join rel) {
        if (rel.getJoinType() != JoinRelType.INNER
                && rel.getJoinType() != JoinRelType.LEFT
                && rel.getJoinType() != JoinRelType.RIGHT
                && rel.getJoinType() != JoinRelType.FULL
                && rel.getJoinType() != JoinRelType.SEMI) {
            // Fall-back to calcite's implementation.
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
        }

        JoinInfo joinInfo = rel.analyzeCondition();

        if (joinInfo.pairs().isEmpty()) {
            // Fall-back to calcite's implementation.
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
        }

        Double leftRowCount = mq.getRowCount(rel.getLeft());
        Double rightRowCount = mq.getRowCount(rel.getRight());

        if (leftRowCount == null || rightRowCount == null) {
            return null;
        }

        // Row count estimates of 0 will be rounded up to 1.
        // So, use maxRowCount where the product is very small.
        if (leftRowCount <= 1.0 || rightRowCount <= 1.0) {
            Double max = mq.getMaxRowCount(rel);
            if (max != null && max <= 1.0) {
                return max;
            }
        }

        Int2ObjectMap<KeyColumnOrigin> columnsFromLeft = resolveOrigins(mq, rel.getLeft(), joinInfo.leftKeys);
        Int2ObjectMap<KeyColumnOrigin> columnsFromRight = resolveOrigins(mq, rel.getRight(), joinInfo.rightKeys);

        Map<TablesPair, JoinContext> joinContexts = new HashMap<>();
        for (IntPair joinKeys : joinInfo.pairs()) {
            KeyColumnOrigin leftKey = columnsFromLeft.get(joinKeys.source);
            KeyColumnOrigin rightKey = columnsFromRight.get(joinKeys.target);

            if (leftKey == null || rightKey == null) {
                continue;
            }

            joinContexts.computeIfAbsent(
                    new TablesPair(
                            leftKey.origin.getOriginTable(),
                            rightKey.origin.getOriginTable()
                    ),
                    key -> {
                        IgniteTable leftTable = key.left.unwrap(IgniteTable.class);
                        IgniteTable rightTable = key.right.unwrap(IgniteTable.class);

                        assert leftTable != null && rightTable != null;

                        int leftPkSize = leftTable.keyColumns().size();
                        int rightPkSize = rightTable.keyColumns().size();

                        return new JoinContext(leftPkSize, rightPkSize);
                    }
            ).countKeys(leftKey, rightKey);
        }

        if (joinContexts.isEmpty()) {
            // Fall-back to calcite's implementation.
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
        }

        Iterator<JoinContext> it = joinContexts.values().iterator();
        JoinContext context = it.next();
        while (it.hasNext()) {
            JoinContext nextContext = it.next();
            if (nextContext.joinType().strength > context.joinType().strength) {
                context = nextContext;
            }

            if (context.joinType().strength == JoiningRelationType.PK_ON_PK.strength) {
                break;
            }
        }

        if (context.joinType() == JoiningRelationType.UNKNOWN) {
            // Fall-back to calcite's implementation.
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
        }

        double postFiltrationAdjustment = 1.0;

        switch (rel.getJoinType()) {
            case INNER:
            case SEMI:
                // Extra join keys as well as non-equi conditions serves as post-filtration,
                // therefore we need to adjust final result with a little factor.
                postFiltrationAdjustment = joinContexts.size() == 1 && joinInfo.isEqui() ? 1.0 : NON_EQUI_COEFF;

                break;
            default:
                break;
        }

        double baseRowCount = 0.0;
        Double percentageAdjustment = null;
        if (context.joinType() == JoiningRelationType.PK_ON_PK) {
            if (rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.SEMI) {
                // Assume we have two fact tables SALES and RETURNS sharing the same primary key. Every item
                // can be sold, but only items which were sold can be returned back, therefore
                // size(SALES) > size(RETURNS). When joining SALES on RETURNS by primary key, the estimated
                // result size will be the same as the size of the smallest table (RETURNS in this case),
                // adjusted by the percentage of rows of the biggest table (SALES in this case; percentage
                // adjustment is required to account for predicates pushed down to the table, e.g. we are
                // interested in returns of items with certain category)
                if (leftRowCount > rightRowCount) {
                    baseRowCount = rightRowCount;
                    percentageAdjustment = mq.getPercentageOriginalRows(rel.getLeft());
                } else {
                    baseRowCount = leftRowCount;
                    percentageAdjustment = mq.getPercentageOriginalRows(rel.getRight());
                }
            } else if (rel.getJoinType() == JoinRelType.LEFT) {
                baseRowCount = leftRowCount;
            } else if (rel.getJoinType() == JoinRelType.RIGHT) {
                baseRowCount = rightRowCount;
            } else if (rel.getJoinType() == JoinRelType.FULL) {
                Double selectivity = mq.getSelectivity(rel, rel.getCondition());

                // Fall-back to calcite's implementation.
                if (selectivity == null) {
                    return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
                }

                baseRowCount = rightRowCount + leftRowCount;
                percentageAdjustment = 1.0 - selectivity;
            }
        } else if (context.joinType() == JoiningRelationType.FK_ON_PK) {
            // For foreign key joins the base table is the one which is joined by non-primary key columns.
            if (rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.SEMI) {
                baseRowCount = leftRowCount;
                percentageAdjustment = mq.getPercentageOriginalRows(rel.getRight());
            } else if (rel.getJoinType() == JoinRelType.LEFT || rel.getJoinType() == JoinRelType.RIGHT) {
                baseRowCount = leftRowCount;
            } else if (rel.getJoinType() == JoinRelType.FULL) {
                Double selectivity = mq.getSelectivity(rel, rel.getCondition());

                // Fall-back to calcite's implementation.
                if (selectivity == null) {
                    return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
                }

                baseRowCount = rightRowCount + leftRowCount;
                percentageAdjustment = 1.0 - selectivity;
            }
        } else { // PK_ON_FK
            if (rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.SEMI) {
                baseRowCount = rightRowCount;
                percentageAdjustment = mq.getPercentageOriginalRows(rel.getLeft());
            } else if (rel.getJoinType() == JoinRelType.RIGHT || rel.getJoinType() == JoinRelType.LEFT) {
                baseRowCount = rightRowCount;
            } else if (rel.getJoinType() == JoinRelType.FULL) {
                Double selectivity = mq.getSelectivity(rel, rel.getCondition());

                // Fall-back to calcite's implementation.
                if (selectivity == null) {
                    return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
                }

                baseRowCount = rightRowCount + leftRowCount;
                percentageAdjustment = 1.0 - selectivity;
            }
        }

        if (percentageAdjustment == null) {
            percentageAdjustment = 1.0; // No info, let's be conservative
        }

        return baseRowCount * percentageAdjustment * postFiltrationAdjustment;
    }

    private static Int2ObjectMap<KeyColumnOrigin> resolveOrigins(RelMetadataQuery mq, RelNode joinShoulder, ImmutableIntList keys) {
        Int2ObjectMap<KeyColumnOrigin> origins = new Int2ObjectOpenHashMap<>();
        for (int i : keys) {
            if (origins.containsKey(i)) {
                continue;
            }

            RelColumnOrigin origin = mq.getColumnOrigin(joinShoulder, i);
            if (origin == null) {
                continue;
            }

            IgniteTable table = origin.getOriginTable().unwrap(IgniteTable.class);
            if (table == null) {
                continue;
            }

            int positionInKey = table.keyColumns().indexOf(origin.getOriginColumnOrdinal());
            origins.put(i, new KeyColumnOrigin(origin, positionInKey));
        }

        return origins;
    }

    private static class KeyColumnOrigin {
        private final RelColumnOrigin origin;
        private final int positionInKey;

        KeyColumnOrigin(RelColumnOrigin origin, int positionInKey) {
            this.origin = origin;
            this.positionInKey = positionInKey;
        }
    }

    private static class TablesPair {
        private final RelOptTable left;
        private final RelOptTable right;

        TablesPair(RelOptTable left, RelOptTable right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TablesPair that = (TablesPair) o;
            // Reference equality on purpose.
            return left == that.left && right == that.right;
        }

        @Override
        public int hashCode() {
            return Objects.hash(left, right);
        }
    }

    private static class JoinContext {
        /** Used columns of primary key of table from left side. */
        private final BitSet leftKeys;
        /** Used columns of primary key of table from right side. */
        private final BitSet rightKeys;

        /**
         * Used columns of primary key in both tables.
         *
         * <p>This bitset is initialized when PK of both tables has equal columns count, and
         * bits are cleared when join pair contains columns with equal positions in PK of corresponding
         * table. For example, having tables T1 and T2 with primary keys in both tables defined as
         * CONSTRAINT PRIMARY KEY (a, b), in case of query
         * {@code SELECT ... FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b} commonKeys will be initialized
         * and cleared, but in case of query {@code SELECT ... FROM t1 JOIN t2 ON t1.a = t2.b AND t1.b = t2.a}
         * (mind the join condition, where column A of one table compared with column B of another), will be 
         * only initialized (since size of the primary keys are equal), but not cleared.
         */
        private final @Nullable BitSet commonKeys;

        JoinContext(int leftPkSize, int rightPkSize) {
            this.leftKeys = new BitSet();
            this.rightKeys = new BitSet();
            this.commonKeys = leftPkSize == rightPkSize ? new BitSet() : null;

            leftKeys.set(0, leftPkSize);
            rightKeys.set(0, rightPkSize);

            if (commonKeys != null) {
                assert leftPkSize == rightPkSize;

                commonKeys.set(0, leftPkSize);
            }
        }

        void countKeys(KeyColumnOrigin left, KeyColumnOrigin right) {
            if (left.positionInKey >= 0) {
                leftKeys.clear(left.positionInKey);
            }

            if (right.positionInKey >= 0) {
                rightKeys.clear(right.positionInKey);
            }

            if (commonKeys != null && left.positionInKey == right.positionInKey && left.positionInKey >= 0) {
                commonKeys.clear(left.positionInKey);
            }
        }

        JoiningRelationType joinType() {
            if (commonKeys != null && commonKeys.isEmpty()) {
                return JoiningRelationType.PK_ON_PK;
            }

            if (rightKeys.isEmpty()) {
                return JoiningRelationType.FK_ON_PK;
            }

            if (leftKeys.isEmpty()) {
                return JoiningRelationType.PK_ON_FK;
            }

            return JoiningRelationType.UNKNOWN;
        }
    }

    /** Enumeration of join types by their semantic. */
    private enum JoiningRelationType {
        /**
         * Join by non-primary key columns.
         *
         * <p>Semantic is unknown.
         */
        UNKNOWN(0),
        /**
         * Join by primary keys on non-primary keys.
         *
         * <p>Currently we don't support Foreign Keys, thus we will assume such types of joins
         * as joins by foreign key.
         */
        PK_ON_FK(UNKNOWN.strength + 1),
        /**
         * Join by non-primary keys on primary keys.
         * 
         * <p>Currently we don't support Foreign Keys, thus we will assume such types of joins
         * as joins by foreign key.
         */
        FK_ON_PK(PK_ON_FK.strength + 1),
        /**
         * Join of two tables which sharing the same primary key.
         *
         * <p>For example, join of tables CATALOG_SALES and CATALOG_RETURN from TPC-DS suite: both tables
         * have the same primary key (ITEM_ID, ORDER_ID).
         */
        PK_ON_PK(FK_ON_PK.strength + 1);

        // The higher the better.
        private final int strength;

        JoiningRelationType(int strength) {
            this.strength = strength;
        }
    }
}
