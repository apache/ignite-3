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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.jetbrains.annotations.Nullable;

/**
 * Base interface for data sources such as tables and views.
 */
public interface IgniteDataSource extends TranslatableTable, Wrapper {

    /**
     * Returns an id of the table.
     *
     * @return And id of the table.
     */
    int id();

    /**
     * Returns the version of the table's schema. The version is bumped only, when table structure was modified (e.g. column added/dropped).
     *
     * @return the version of the table's schema.
     */
    int version();

    /**
     * Return table descriptor modification timestamp.
     */
    long timestamp();

    /**
     * Gets a name of the table.
     *
     * @return Table name.
     */
    String name();

    /**
     * Returns a descriptor of the table.
     *
     * @return A descriptor of the table.
     */
    TableDescriptor descriptor();

    /** {@inheritDoc} */
    @Override
    default RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return getRowType(typeFactory, null);
    }

    /**
     * Returns new type according {@code requiredColumns} param.
     *
     * @param typeFactory     Factory.
     * @param requiredColumns Used columns enumeration.
     */
    RelDataType getRowType(RelDataTypeFactory typeFactory, @Nullable ImmutableIntList requiredColumns);

    /**
     * Returns distribution of this data source.
     *
     * @return Table distribution.
     */
    IgniteDistribution distribution();
}
