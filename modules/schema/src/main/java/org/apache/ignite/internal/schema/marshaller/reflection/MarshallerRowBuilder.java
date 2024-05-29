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

import java.util.List;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.jetbrains.annotations.Nullable;

/**
 * Helper to build a binary row.
 */
abstract class MarshallerRowBuilder {
    /** Build binary row. */
    abstract BinaryRow build() throws MarshallerException;

    /** Creates binary row builder for key only. */
    static MarshallerRowBuilder createRowBuilder(SchemaDescriptor schema, Marshaller keyMarsh, Object key) {
        ObjectStatistics statistics = ObjectStatistics.collectObjectStats(schema, schema.keyColumns(), keyMarsh, key);

        RowAssembler assembler = new RowAssembler(schema.version(), schema.keyColumns(), statistics.estimatedValueSize());

        return new KeyOnlyBuilder(assembler, keyMarsh, statistics.values());
    }

    /** Creates binary row builder. */
    static MarshallerRowBuilder createRowBuilder(SchemaDescriptor schema, Marshaller keyMarsh, Marshaller valMarsh, Object key,
            @Nullable Object val) {
        ObjectStatistics keyStat = ObjectStatistics.collectObjectStats(schema, schema.keyColumns(), keyMarsh, key);
        int keyLen = schema.keyColumns().size();

        Object[] values = new Object[schema.columns().size()];

        for (int i = 0; i < keyLen; i++) {
            values[schema.keyColumns().get(i).positionInRow()] = keyStat.value(i);
        }

        ObjectStatistics valStat = ObjectStatistics.collectObjectStats(schema, schema.valueColumns(), valMarsh, val);

        for (int i = 0; i < schema.valueColumns().size(); i++) {
            values[schema.valueColumns().get(i).positionInRow()] = valStat.value(i);
        }

        int totalValueSize;
        if (keyStat.estimatedValueSize() < 0 || valStat.estimatedValueSize() < 0) {
            totalValueSize = -1;
        } else {
            totalValueSize = keyStat.estimatedValueSize() + valStat.estimatedValueSize();
        }

        RowAssembler assembler = new RowAssembler(schema, totalValueSize);

        return new KeyValueBuilder(schema, assembler, keyMarsh, valMarsh, values);
    }

    static class KeyOnlyBuilder extends MarshallerRowBuilder {
        final RowAssembler asm;
        final Marshaller keyMarsh;
        final Object[] values;

        KeyOnlyBuilder(RowAssembler asm, Marshaller keyMarsh, Object[] values) {
            this.asm = asm;
            this.keyMarsh = keyMarsh;
            this.values = values;
        }

        @Override
        BinaryRow build() throws MarshallerException {
            RowWriter writer = new RowWriter(asm);

            keyMarsh.writeFieldValues(values, writer);

            return asm.build();
        }
    }

    static class KeyValueBuilder extends KeyOnlyBuilder {
        private final SchemaDescriptor schema;
        private final Marshaller valMarsh;

        KeyValueBuilder(SchemaDescriptor schema, RowAssembler asm, Marshaller keyMarsh, Marshaller valMarsh, Object[] values) {
            super(asm, keyMarsh, values);

            this.valMarsh = valMarsh;
            this.schema = schema;
        }

        @Override
        BinaryRow build() throws MarshallerException {
            RowWriter writer = new RowWriter(asm);

            List<Column> columns = schema.columns();

            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);

                if (column.positionInKey() >= 0) {
                    keyMarsh.writeFieldValue(writer, column.positionInKey(), values[i]);
                } else {
                    valMarsh.writeFieldValue(writer, column.positionInValue(), values[i]);
                }
            }

            return asm.build();
        }
    }
}
