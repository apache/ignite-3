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

package org.apache.ignite.internal.sql.engine.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Row type utils class.
 */
public final class RowTypeUtils {
    /**
     * Computes row type for provided table with virtual columns filtering.
     *
     * @param tableDescriptor Table descriptor.
     * @param factory Type factory.
     * @return Row type.
     */
    public static RelDataType rowType(TableDescriptor tableDescriptor, IgniteTypeFactory factory) {
        return tableDescriptor.rowType(factory, storedColumns(tableDescriptor));
    }

    /**
     * Compute stored columns count for provided table.
     *
     * @param tableDescriptor Table descriptor.
     * @return Stored columns count.
     */
    public static int storedColumnsCount(TableDescriptor tableDescriptor) {
        int count = 0;
        for (ColumnDescriptor descriptor : tableDescriptor) {
            count += descriptor.virtual() ? 0 : 1;
        }
        return count;
    }

    private static @Nullable ImmutableIntList storedColumns(TableDescriptor tableDescriptor) {
        if (!tableDescriptor.hasVirtualColumns()) {
            return null;
        }

        IntArrayList storedColumns = new IntArrayList(tableDescriptor.columnsCount());

        for (ColumnDescriptor descriptor : tableDescriptor) {
            if (!descriptor.virtual()) {
                storedColumns.add(descriptor.logicalIndex());
            }
        }

        return ImmutableIntList.of(storedColumns.toIntArray());
    }
}
