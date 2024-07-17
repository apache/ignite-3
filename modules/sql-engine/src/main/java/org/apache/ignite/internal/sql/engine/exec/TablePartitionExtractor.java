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

package org.apache.ignite.internal.sql.engine.exec;

import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypeSpec;

/** Extract partition based on supplied row.  */
public class TablePartitionExtractor<RowT> implements RowPartitionExtractor<RowT> {
    private final PartitionCalculator partitionCalculator;
    private final int[] fields;
    private final TableDescriptor tableDescriptor;
    private final RowHandler<RowT> rowHandler;

    /** Constructor. */
    public TablePartitionExtractor(
            PartitionCalculator partitionCalculator,
            int[] fields,
            TableDescriptor tableDescriptor,
            RowHandler<RowT> rowHandler
    ) {
        this.partitionCalculator = partitionCalculator;
        this.fields = fields;
        this.tableDescriptor = tableDescriptor;
        this.rowHandler = rowHandler;
    }

    /** {@inheritDoc} */
    @Override
    public int partition(RowT row) {
        int[] colocationColumns = tableDescriptor.distribution().getKeys().toIntArray();

        for (int i = 0; i < fields.length; i++) {
            Object value = rowHandler.get(fields[i], row);

            NativeTypeSpec nativeTypeSpec = tableDescriptor.columnDescriptor(colocationColumns[i]).physicalType().spec();
            Class<?> storageType = NativeTypeSpec.toClass(nativeTypeSpec, true);
            value = TypeUtils.fromInternal(value, storageType);
            partitionCalculator.append(value);
        }

        return partitionCalculator.partition();
    }
}
