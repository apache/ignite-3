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
     * Compute stored rows count for provided table.
     *
     * @param tableDescriptor Table descriptor.
     * @return Stored rows count.
     */
    public static int storedRowsCount(TableDescriptor tableDescriptor) {
        int count = 0;
        for (ColumnDescriptor descriptor : tableDescriptor) {
            count += descriptor.virtual() ? 0 : 1;
        }
        return count;
    }

    private static ImmutableIntList storedColumns(TableDescriptor tableDescriptor) {
        IntArrayList storedColumns = new IntArrayList(tableDescriptor.columnsCount());

        // TODO: IGNITE-22703 Let's add a flag to descriptor and run this check only once.
        boolean virtualColumnFound = false;
        for (ColumnDescriptor descriptor : tableDescriptor) {
            if (!descriptor.virtual()) {
                storedColumns.add(descriptor.logicalIndex());
            } else {
                virtualColumnFound = true;
            }
        }

        if (virtualColumnFound) {
            return ImmutableIntList.of(storedColumns.toIntArray());
        }

        return null;
    }
}
