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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.ArrayUtils.INT_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;

import java.util.Objects;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.util.ColocationUtils;
import org.apache.ignite.internal.util.HashCalculator;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/** Partition assignments resolver. */
public class PartitionResolverImpl<RowT> implements RowAwareAssignmentResolver<RowT>, AssignmentsResolver<RowT> {
    private final RowHandler<RowT> rowHandler;
    private HashCalculator hashCalc;
    private int curColIdx;
    protected final int partitions;

    protected int[] fields;
    protected NativeType[] fieldTypes;

    private boolean calculated;

    /** Constructor. */
    public PartitionResolverImpl(int partitions, TableDescriptor tableDescriptor, RowHandler<RowT> rowHandler) {
        this.rowHandler = Objects.requireNonNull(rowHandler, "rowHandler");
        this.partitions = partitions;

        ImmutableIntList colocationColumns = tableDescriptor.distribution().getKeys();
        int fieldCnt = colocationColumns.size();
        fieldTypes = new NativeType[fieldCnt];

        for (int i = 0; i < fieldCnt; i++) {
            ColumnDescriptor colDesc = tableDescriptor.columnDescriptor(colocationColumns.getInt(i));

            fieldTypes[i] = colDesc.physicalType();
        }

        fields = colocationColumns.toIntArray();
    }

    /** {@inheritDoc} */
    @Override
    public void append(@Nullable Object value) {
        hashCalc = initCalculator();
        assert curColIdx < fields.length : "extra keys supplied";

        ColocationUtils.append(hashCalc, value, fieldTypes[curColIdx++]);
    }

    /** {@inheritDoc} */
    @Override
    public int getPartition(RowT row) {
        initCalculator();

        return IgniteUtils.safeAbs(hashOf(row) % partitions);
    }

    /** {@inheritDoc} */
    @Override
    public int getPartition() {
        assert hashCalc != null;
        assert curColIdx == fields.length :
                format("partially initialized: keys supplied={}, keys avoid={}", curColIdx, fields.length);
        return IgniteUtils.safeAbs(calculate() % partitions);
    }

    private int hashOf(RowT row) {
        for (int i = 0; i < fields.length; i++) {
            Object value = rowHandler.get(fields[i], row);
            NativeTypeSpec nativeTypeSpec = fieldTypes[i].spec();
            Class<?> storageType = NativeTypeSpec.toClass(nativeTypeSpec, true);

            value = TypeUtils.fromInternal(value, storageType);
            append(value);
        }

        return calculate();
    }

    private HashCalculator initCalculator() {
        if (hashCalc == null || calculated) {
            hashCalc = new HashCalculator();
            calculated = false;
            curColIdx = 0;
        }
        return hashCalc;
    }

    private int calculate() {
        assert curColIdx == fields.length;
        if (!calculated) {
            fields = INT_EMPTY_ARRAY;
            fieldTypes = (NativeType[]) OBJECT_EMPTY_ARRAY;
        }
        calculated = true;
        return hashCalc.hash();
    }
}
