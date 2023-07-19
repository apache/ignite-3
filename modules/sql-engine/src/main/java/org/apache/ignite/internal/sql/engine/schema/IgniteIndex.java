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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;

/**
 * Auxiliary data structure to represent a table index.
 */
public class IgniteIndex {
    /**
     * Collation, or sorting order, of a column.
     */
    public enum Collation {
        ASC_NULLS_FIRST(true, true),
        ASC_NULLS_LAST(true, false),
        DESC_NULLS_FIRST(false, true),
        DESC_NULLS_LAST(false, false);

        /** Returns collation for a given specs. */
        public static Collation of(boolean asc, boolean nullsFirst) {
            return asc ? nullsFirst ? ASC_NULLS_FIRST : ASC_NULLS_LAST
                    : nullsFirst ? DESC_NULLS_FIRST : DESC_NULLS_LAST;
        }

        public final boolean asc;

        public final boolean nullsFirst;

        Collation(boolean asc, boolean nullsFirst) {
            this.asc = asc;
            this.nullsFirst = nullsFirst;
        }
    }

    private final int indexId;

    private final String name;

    private final IgniteDistribution tableDistribution;

    private final RelCollation collation;

    private final Type type;

    /** Constructor. */
    public IgniteIndex(int indexId, String name, Type type, IgniteDistribution tableDistribution, RelCollation collation) {
        this.indexId = indexId;
        this.name = name;
        this.type = type;
        this.tableDistribution = tableDistribution;
        this.collation = collation;
    }

    /** Returns the name of this index. */
    public String name() {
        return name;
    }

    /** Returns the type of this index. */
    public Type type() {
        return type;
    }

    /** Returns index id. */
    public int id() {
        return indexId;
    }

    /** Returns the collation of this index. */
    public RelCollation collation() {
        return collation;
    }

    /**
     * Translates this index into relational operator.
     */
    public IgniteLogicalIndexScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTable,
            List<RexNode> proj,
            RexNode condition,
            ImmutableBitSet requiredCols
    ) {
        RelTraitSet traitSet = cluster.traitSetOf(Convention.Impl.NONE)
                .replace(tableDistribution)
                .replace(type() == Type.HASH ? RelCollations.EMPTY : collation);

        return IgniteLogicalIndexScan.create(cluster, traitSet, relOptTable, name, proj, condition, requiredCols);
    }

    /**
     * Type of the index.
     */
    public enum Type {
        HASH, SORTED
    }
}
