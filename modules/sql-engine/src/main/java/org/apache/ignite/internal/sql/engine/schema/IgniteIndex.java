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

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.native2relationalType;

import java.util.ArrayList;
import java.util.Comparator;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Schema object representing an Index.
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


    /**
     * Type of the index.
     */
    public enum Type {
        HASH, SORTED;
    }

    private final int id;

    private final String name;

    private final IgniteDistribution tableDistribution;

    private final RelCollation outputCollation;

    private final RelCollation indexCollation;

    private final Type type;

    private RelDataType rowType;

    /** Constructor. */
    public IgniteIndex(int id, String name, Type type, IgniteDistribution tableDistribution, RelCollation outputCollation) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.tableDistribution = tableDistribution;
        this.outputCollation = outputCollation;

        indexCollation = (type == Type.SORTED) ? createIndexCollation(outputCollation.getFieldCollations()) : null;
    }

    /** Returns an id of the index. */
    public int id() {
        return id;
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
        return outputCollation;
    }

    /** Returns index row type. */
    public RelDataType rowType(IgniteTypeFactory factory, TableDescriptor tableDescriptor) {
        if (rowType == null) {
            rowType = createRowType(factory, tableDescriptor, outputCollation);
        }
        return rowType;
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
                .replace(type() == Type.HASH ? RelCollations.EMPTY : outputCollation);

        return IgniteLogicalIndexScan.create(cluster, traitSet, relOptTable, name, proj, condition, requiredCols);
    }

    static RelCollation createIndexCollation(CatalogIndexDescriptor descriptor, TableDescriptor tableDescriptor) {
        if (descriptor instanceof CatalogSortedIndexDescriptor) {
            CatalogSortedIndexDescriptor sortedIndexDescriptor = (CatalogSortedIndexDescriptor) descriptor;
            List<CatalogIndexColumnDescriptor> columns = sortedIndexDescriptor.columns();
            List<RelFieldCollation> fieldCollations = new ArrayList<>(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                CatalogIndexColumnDescriptor column = columns.get(i);
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
        } else if (descriptor instanceof CatalogHashIndexDescriptor) {
            CatalogHashIndexDescriptor hashIndexDescriptor = (CatalogHashIndexDescriptor) descriptor;
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

    /**
     * Creates {@link RelCollation} object from a given collations.
     *
     * @param collations Original collation.
     * @return a {@link RelCollation} object.
     */
    private static RelCollation createIndexCollation(List<RelFieldCollation> collations) {
        int i = 0;
        List<RelFieldCollation> result = new ArrayList<>(collations.size());
        for (RelFieldCollation fieldCollation : collations) {
            result.add(fieldCollation.withFieldIndex(i++));
        }

        return RelCollations.of(result);
    }

    //TODO: cache rowType as it can't be changed.

    /**
     * Returns index row type.
     *
     * <p>This is a struct type whose fields describe the names and types of indexed columns.</p>
     *
     * <p>The implementer must use the type factory provided. This ensures that
     * the type is converted into a canonical form; other equal types in the same query will use the same object.</p>
     *
     * @param typeFactory Type factory with which to create the type
     * @param tableDescriptor Table descriptor.
     * @param collation Index collation.
     * @return Row type.
     */
    public static RelDataType createRowType(IgniteTypeFactory typeFactory, TableDescriptor tableDescriptor, RelCollation collation) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(typeFactory);

        for (RelFieldCollation field : collation.getFieldCollations()) {
            ColumnDescriptor colDesc = tableDescriptor.columnDescriptor(field.getFieldIndex());
            b.add(colDesc.name(), native2relationalType(typeFactory, colDesc.physicalType(), colDesc.nullable()));
        }

        return b.build();
    }

    /** Index row collation. */
    public static <RowT> @Nullable Comparator<RowT> createComparator(ExpressionFactory<RowT> factory, IgniteIndex index) {
        if (index.type != Type.SORTED) {
            return null;
        }

        return factory.comparator(index.indexCollation);
    }
}
