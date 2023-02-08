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

package org.apache.ignite.internal.schema.marshaller.reflection;

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.row.RowAssembler;

/**
 * Object statistic.
 */
class ObjectStatistics {
    /** Cached zero statistics. */
    private static final ObjectStatistics ZERO_STATISTICS = new ObjectStatistics(true, -1);

    /** Whether there are NULL values or not. */
    private final boolean hasNulls;

    /** Estimated total size of the object. */
    private final int estimatedValueSize;

    /** Constructor. */
    private ObjectStatistics(boolean hasNulls, int estimatedValueSize) {
        this.hasNulls = hasNulls;
        this.estimatedValueSize = estimatedValueSize;
    }

    boolean hasNulls() {
        return hasNulls;
    }

    int getEstimatedValueSize() {
        return estimatedValueSize;
    }

    /**
     * Reads object fields and gather statistic.
     *
     * @throws MarshallerException If failed to read object content.
     */
    private static ObjectStatistics collectObjectStats(Columns cols, Marshaller marsh, Object obj) throws MarshallerException {
        if (obj == null) {
            return ZERO_STATISTICS;
        }

        boolean hasNulls = false;
        int estimatedValueSize = 0;

        for (int i = 0; i < cols.length(); i++) {
            Object val = marsh.value(obj, i);
            NativeType colType = cols.column(i).type();

            if (val == null) {
                hasNulls = true;
            } else if (colType.spec().fixedLength()) {
                estimatedValueSize += colType.sizeInBytes();
            } else {
                estimatedValueSize += getValueSize(val, colType);
            }
        }

        return new ObjectStatistics(hasNulls, estimatedValueSize);
    }

    static RowAssembler createAssembler(SchemaDescriptor schema, Marshaller keyMarsh, Object key)
            throws MarshallerException {
        ObjectStatistics keyStat = collectObjectStats(schema.keyColumns(), keyMarsh, key);

        boolean hasNulls = keyStat.hasNulls();
        int totalValueSize = keyStat.getEstimatedValueSize();

        return new RowAssembler(schema.keyColumns(), null, schema.version(), hasNulls, totalValueSize);
    }

    static RowAssembler createAssembler(SchemaDescriptor schema, Marshaller keyMarsh, Marshaller valMarsh, Object key, Object val)
            throws MarshallerException {
        ObjectStatistics keyStat = collectObjectStats(schema.keyColumns(), keyMarsh, key);
        ObjectStatistics valStat = collectObjectStats(schema.valueColumns(), valMarsh, val);

        boolean hasNulls = keyStat.hasNulls() || valStat.hasNulls();
        int totalValueSize;
        if (keyStat.getEstimatedValueSize() < 0 || valStat.getEstimatedValueSize() < 0) {
            totalValueSize = -1;
        } else {
            totalValueSize = keyStat.getEstimatedValueSize() + valStat.getEstimatedValueSize();
        }

        return new RowAssembler(schema, hasNulls, totalValueSize);
    }
}
