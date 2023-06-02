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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.catalog.descriptors.HashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.IndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SortedIndexDescriptor;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;

/**
 * Index.
 */
public class IgniteSchemaIndex {

    private final String name;

    private final IgniteDistribution tableDistribution;

    private final RelCollation collation;

    private final Type type;

    /** Constructor. */
    public IgniteSchemaIndex(String name, Type type, IgniteDistribution tableDistribution, RelCollation collation) {
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

    static RelCollation createIndexCollation(IndexDescriptor descriptor, TableDescriptor tableDescriptor) {
        if (descriptor instanceof SortedIndexDescriptor) {
            SortedIndexDescriptor sortedIndexDescriptor = (SortedIndexDescriptor) descriptor;
            List<IndexColumnDescriptor> columns = sortedIndexDescriptor.columns();
            List<RelFieldCollation> fieldCollations = new ArrayList<>(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                IndexColumnDescriptor column = columns.get(i);
                ColumnDescriptor columnDesc = tableDescriptor.columnDescriptor(column.name());
                int fieldIndex = columnDesc.logicalIndex();

                RelFieldCollation fieldCollation;
                switch (column.collation()) {
                    case ASC_NULLS_FIRST:
                        fieldCollation = new RelFieldCollation(fieldIndex, Direction.ASCENDING, NullDirection.FIRST);
                        break;
                    case ASC_NULLS_LAST:
                        fieldCollation = new RelFieldCollation(fieldIndex, Direction.ASCENDING, NullDirection.LAST);
                        break;
                    case DESC_NULLS_FIRST:
                        fieldCollation = new RelFieldCollation(fieldIndex, Direction.DESCENDING, NullDirection.FIRST);
                        break;
                    case DESC_NULLS_LAST:
                        fieldCollation = new RelFieldCollation(fieldIndex, Direction.DESCENDING, NullDirection.LAST);
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected collation: " + column.collation());
                }

                fieldCollations.add(fieldCollation);
            }

            return RelCollations.of(fieldCollations);
        } else if (descriptor instanceof HashIndexDescriptor) {
            HashIndexDescriptor hashIndexDescriptor = (HashIndexDescriptor) descriptor;
            List<String> columns = hashIndexDescriptor.columns();
            List<RelFieldCollation> fieldCollations = new ArrayList<>(columns.size());

            for (String columnName : columns) {
                ColumnDescriptor columnDesc = tableDescriptor.columnDescriptor(columnName);

                fieldCollations.add(new RelFieldCollation(columnDesc.logicalIndex(), Direction.CLUSTERED, NullDirection.UNSPECIFIED));
            }

            return RelCollations.of(fieldCollations);
        } else {
            throw new IllegalArgumentException("Unexpected index type: " + descriptor);
        }
    }
}
