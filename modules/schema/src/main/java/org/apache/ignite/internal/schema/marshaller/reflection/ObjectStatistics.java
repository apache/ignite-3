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

import java.math.BigDecimal;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.jetbrains.annotations.Nullable;

/**
 * Object statistic.
 */
class ObjectStatistics {
    /** Cached zero statistics. */
    private static final ObjectStatistics ZERO_STATISTICS = new ObjectStatistics(-1, false);

    /**
     * The average size of a decimal number is taken from the air. This value is used to avoid unnecessary
     * buffer reallocation when building a binary row. The exact size of the decimal value can be determined
     * after compaction (see {@link BinaryTupleCommon#shrinkDecimal(BigDecimal, int)}.
     */
    private static final int DECIMAL_VALUE_SIZE = 32;

    /** Estimated total size of the object. */
    private final int estimatedValueSize;

    /** flag indicating that the size is calculated exactly or approximately. */
    private final boolean exactEstimation;

    /** Constructor. */
    private ObjectStatistics(int estimatedValueSize, boolean exactEstimation) {
        this.estimatedValueSize = estimatedValueSize;
        this.exactEstimation = exactEstimation;
    }

    private int estimatedValueSize() {
        return estimatedValueSize;
    }

    private boolean exactEstimation() {
        return exactEstimation;
    }

    /**
     * Reads object fields and gather statistic.
     */
    private static ObjectStatistics collectObjectStats(List<Column> cols, Marshaller marsh, @Nullable Object obj) {
        if (obj == null) {
            return ZERO_STATISTICS;
        }

        int estimatedValueSize = 0;
        boolean exactEstimation = true;

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
                int valueSize = colType.spec() == NativeTypeSpec.DECIMAL ? DECIMAL_VALUE_SIZE : getValueSize(val, colType);

                exactEstimation = false;

                estimatedValueSize += valueSize;
            }
        }

        return new ObjectStatistics(estimatedValueSize, exactEstimation);
    }

    static RowAssembler createAssembler(SchemaDescriptor schema, Marshaller keyMarsh, Object key) {
        ObjectStatistics keyStat = collectObjectStats(schema.keyColumns(), keyMarsh, key);

        int totalValueSize = keyStat.estimatedValueSize();

        return new RowAssembler(schema.version(), schema.keyColumns(), totalValueSize, keyStat.exactEstimation());
    }

    static RowAssembler createAssembler(
            SchemaDescriptor schema, Marshaller keyMarsh, Marshaller valMarsh, Object key, @Nullable Object val) {
        ObjectStatistics keyStat = collectObjectStats(schema.keyColumns(), keyMarsh, key);
        ObjectStatistics valStat = collectObjectStats(schema.valueColumns(), valMarsh, val);

        int totalValueSize;
        if (keyStat.estimatedValueSize() < 0 || valStat.estimatedValueSize() < 0) {
            totalValueSize = -1;
        } else {
            totalValueSize = keyStat.estimatedValueSize() + valStat.estimatedValueSize();
        }

        return new RowAssembler(schema.version(), schema.columns(), totalValueSize, keyStat.exactEstimation() && valStat.exactEstimation());
    }
}
