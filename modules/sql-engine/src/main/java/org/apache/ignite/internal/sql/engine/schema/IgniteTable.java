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

import java.util.Map;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;

/**
 * Table representation as object in SQL schema.
 */
public interface IgniteTable extends IgniteDataSource {
    /**
     * Checks whether it is possible to update a column with a given index.
     *
     * @param colIdx Column index.
     * @return {@code True} if update operation is allowed for a column with a given index.
     */
    boolean isUpdateAllowed(int colIdx);

    /**
     * Returns row type excluding effectively virtual or hidden fields.
     *
     * @param factory Type factory.
     * @return Row type for INSERT operation.
     */
    RelDataType rowTypeForInsert(IgniteTypeFactory factory);

    /**
     * Returns row type excluding effectively virtual fields.
     *
     * @param factory Type factory.
     * @return Row type for UPDATE operation.
     */
    RelDataType rowTypeForUpdate(IgniteTypeFactory factory);

    /**
     * Returns row type containing only key fields.
     *
     * @param factory Type factory.
     * @return Row type for DELETE operation.
     */
    RelDataType rowTypeForDelete(IgniteTypeFactory factory);

    /** Return indexes of column representing primary key in the order they are specified in the index. */
    ImmutableIntList keyColumns();

    /**
     * Return partition correspondence calculator.
     */
    Supplier<PartitionCalculator> partitionCalculator();

    /**
     * Returns all table indexes.
     *
     * @return Indexes for the current table.
     */
    Map<String, IgniteIndex> indexes();

    /**
     * Returns the number of partitions for this table.
     *
     * @return Number of partitions.
     */
    int partitions();
}
