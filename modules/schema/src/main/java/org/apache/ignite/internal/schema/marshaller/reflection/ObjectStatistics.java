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
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.type.NativeType;
import org.jetbrains.annotations.Nullable;

/**
 * Object statistic.
 */
class ObjectStatistics {
    /** Estimated total size of the object. */
    private final int estimatedValueSize;

    private final Object[] values;

    /** Constructor. */
    private ObjectStatistics(Object[] values, int estimatedValueSize) {
        this.values = values;
        this.estimatedValueSize = estimatedValueSize;
    }

    int estimatedValueSize() {
        return estimatedValueSize;
    }

    Object[] values() {
        return values;
    }

    <T> T value(int index) {
        return (T) values[index];
    }

    /**
     * Reads object fields and gather statistic.
     */
    static ObjectStatistics collectObjectStats(SchemaDescriptor schema, List<Column> cols, Marshaller marsh, @Nullable Object obj) {
        Object[] vals = new Object[cols.size()];

        if (obj == null) {
            return new ObjectStatistics(vals, -1);
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
                vals[i] = MarshallerUtil.shrinkValue(val, col.type());

                estimatedValueSize += getValueSize(vals[i], colType);
            }
        }

        return new ObjectStatistics(vals, estimatedValueSize);
    }
}
