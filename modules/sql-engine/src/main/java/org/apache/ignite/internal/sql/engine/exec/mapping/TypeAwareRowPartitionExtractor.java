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

import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.util.IgniteUtils;

/** Resolve partition, according colocation fields and it`s types. */
public class TypeAwareRowPartitionExtractor<RowT> implements RowPartitionExtractor<RowT> {
    private final TypeAwareObjectPartitionExtractor<RowT> extractor;

    /** Constructor. */
    public TypeAwareRowPartitionExtractor(int partitions, int[] fields, TableDescriptor tableDescriptor, RowHandler<RowT> rowHandler) {
        extractor = new TypeAwareObjectPartitionExtractor<>(partitions, fields, tableDescriptor, rowHandler);
    }

    /** {@inheritDoc} */
    @Override
    public int getPartition(RowT row) {
        extractor.initCalculator();

        return IgniteUtils.safeAbs(hashOf(row) % extractor.partitions);
    }

    private int hashOf(RowT row) {
        for (int i = 0; i < extractor.fields.length; i++) {
            Object value = extractor.rowHandler.get(extractor.fields[i], row);
            NativeTypeSpec nativeTypeSpec = extractor.fieldTypes[i].spec();
            Class<?> storageType = NativeTypeSpec.toClass(nativeTypeSpec, true);

            value = TypeUtils.fromInternal(value, storageType);
            extractor.append(value);
        }

        return extractor.calculate();
    }
}
