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

package org.apache.ignite.internal.sql.engine.externalize;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Minimal implementation of {@link RelOptSchema} interface.
 * 
 * <p>This is utility class for deserialization purpose. It's used to resolve {@link RelOptTable}'s by its qualified names. Returns
 * minimal implementations of {@link RelOptTable} which can be used to {@link RelOptTable#unwrap} instance of {@link IgniteTable} which
 * is required for composing execution tree.
 */
class RelOptSchemaImpl implements RelOptSchema {
    private final SchemaPlus root;

    RelOptSchemaImpl(SchemaPlus root) {
        this.root = root;
    }

    @Override
    public RelOptTable getTableForMember(List<String> names) {
        if (nullOrEmpty(names) || names.size() != 2) {
            throw new SqlException(Common.INTERNAL_ERR, "Expected name of exactly two parts, but was " + names);
        }

        SchemaPlus schema = root.getSubSchema(names.get(0));

        if (schema == null) {
            throw new SqlException(Common.INTERNAL_ERR, "Schema with name \"" + names.get(0) + "\" not found");
        }

        Table table = schema.getTable(names.get(1));

        if (table == null) {
            throw new SqlException(Common.INTERNAL_ERR, "Table with name " + names + " not found");
        }

        return new RelOptTableImpl(names, (IgniteDataSource) table);
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
        return Commons.typeFactory();
    }

    @Override
    public void registerRules(RelOptPlanner planner) {
        throw new AssertionError("Should not be called");
    }

    private static class RelOptTableImpl implements RelOptTable {
        private final List<String> qualifiedName;
        private final IgniteDataSource igniteDataSource;

        private RelOptTableImpl(List<String> qualifiedName, IgniteDataSource igniteDataSource) {
            this.qualifiedName = qualifiedName;
            this.igniteDataSource = igniteDataSource;
        }

        @Override
        public List<String> getQualifiedName() {
            // used within explainTerms that is widely used in printing out plan or in equality
            return qualifiedName;
        }

        @Override
        public double getRowCount() {
            throw new AssertionError("Should not be called");
        }

        @Override
        public RelDataType getRowType() {
            throw new AssertionError("Should not be called");
        }

        @Override
        public @Nullable RelOptSchema getRelOptSchema() {
            return null;
        }

        @Override
        public RelNode toRel(ToRelContext context) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public @Nullable List<RelCollation> getCollationList() {
            throw new AssertionError("Should not be called");
        }

        @Override
        public @Nullable RelDistribution getDistribution() {
            throw new AssertionError("Should not be called");
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public @Nullable List<ImmutableBitSet> getKeys() {
            throw new AssertionError("Should not be called");
        }

        @Override
        public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
            throw new AssertionError("Should not be called");
        }

        @Override
        public @Nullable Expression getExpression(Class clazz) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public RelOptTable extend(List<RelDataTypeField> extendedFields) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public List<ColumnStrategy> getColumnStrategies() {
            throw new AssertionError("Should not be called");
        }

        @Override
        public <C> @Nullable C unwrap(Class<C> clazz) {
            if (clazz.isAssignableFrom(igniteDataSource.getClass())) {
                return clazz.cast(igniteDataSource);
            }

            return null;
        }
    }
}
