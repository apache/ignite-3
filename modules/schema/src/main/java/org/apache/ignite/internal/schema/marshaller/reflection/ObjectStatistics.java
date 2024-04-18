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

import java.util.List;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.type.NativeType;
import org.jetbrains.annotations.Nullable;

/**
 * Object statistic.
 */
class ObjectStatistics {
    /** Cached zero statistics. */
    private static final ObjectStatistics ZERO_STATISTICS = new ObjectStatistics(-1);

    /** Estimated total size of the object. */
    private final int estimatedValueSize;

    /** Constructor. */
    private ObjectStatistics(int estimatedValueSize) {
        this.estimatedValueSize = estimatedValueSize;
    }

    private int getEstimatedValueSize() {
        return estimatedValueSize;
    }

    /**
     * Reads object fields and gather statistic.
     */
    private static ObjectStatistics collectObjectStats(List<Column> cols, Marshaller marsh, @Nullable Object obj) {
        if (obj == null) {
            return ZERO_STATISTICS;
        }

        int estimatedValueSize = 0;

        for (int i = 0; i < cols.size(); i++) {
            Object val = marsh.value(obj, i);
            Column col = cols.get(i);
            NativeType colType = col.type();

            if (val == null) {
                continue;
            }

            col.validate(val);

            if (colType.spec().fixedLength()) {
                estimatedValueSize += colType.sizeInBytes();
            } else {
                estimatedValueSize += getValueSize(val, colType);
            }
        }

        return new ObjectStatistics(estimatedValueSize);
    }

    static RowAssembler createAssembler(SchemaDescriptor schema, Marshaller keyMarsh, Object key) {
        ObjectStatistics keyStat = collectObjectStats(schema.keyColumns(), keyMarsh, key);

        int totalValueSize = keyStat.getEstimatedValueSize();

        return new RowAssembler(schema.version(), schema.keyColumns(), totalValueSize);
    }

    static RowAssembler createAssembler(
            SchemaDescriptor schema, Marshaller keyMarsh, Marshaller valMarsh, Object key, @Nullable Object val) {
        ObjectStatistics keyStat = collectObjectStats(schema.keyColumns(), keyMarsh, key);
        ObjectStatistics valStat = collectObjectStats(schema.valueColumns(), valMarsh, val);

        int totalValueSize;
        if (keyStat.getEstimatedValueSize() < 0 || valStat.getEstimatedValueSize() < 0) {
            totalValueSize = -1;
        } else {
            totalValueSize = keyStat.getEstimatedValueSize() + valStat.getEstimatedValueSize();
        }

        return new RowAssembler(schema, totalValueSize);
    }
}
