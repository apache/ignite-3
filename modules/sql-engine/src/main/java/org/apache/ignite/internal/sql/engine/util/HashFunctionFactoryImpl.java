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

import static org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl.DEFAULT_VALUE_PLACEHOLDER;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.Objects;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteCatalogSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.util.ColocationUtils;
import org.apache.ignite.internal.util.HashCalculator;

/**
 * Factory for creating a function to calculate the hash of the specified fields of a row.
 */
public class HashFunctionFactoryImpl<T> implements HashFunctionFactory<T> {
    private final RowHandler<T> rowHandler;
    private final IgniteCatalogSchema schema;

    public HashFunctionFactoryImpl(IgniteCatalogSchema schema, RowHandler<T> rowHandler) {
        this.schema = schema;
        this.rowHandler = rowHandler;
    }

    /** {@inheritDoc} */
    @Override
    public RowHashFunction<T> create(int[] fields, int tableId) {
        int fieldCnt = fields.length;
        NativeType[] fieldTypes = new NativeType[fieldCnt];
        //TODO: optimize this
        TableDescriptor tblDesc = schema.getTableNames().stream()
                .map(schema::getTable)
                .map(IgniteTable.class::cast)
                .filter(t -> t.id() == tableId)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No table found: tableId=" + tableId))
                .descriptor();

        ImmutableIntList colocationColumns = tblDesc.distribution().getKeys();

        assert colocationColumns.size() == fieldCnt : "fieldsCount=" + fieldCnt + ", colocationColumns=" + colocationColumns;

        for (int i = 0; i < fieldCnt; i++) {
            ColumnDescriptor colDesc = tblDesc.columnDescriptor(colocationColumns.getInt(i));

            fieldTypes[i] = colDesc.physicalType();
        }

        return new TypesAwareHashFunction<>(fields, fieldTypes, rowHandler);
    }

    /** {@inheritDoc} */
    @Override
    public RowHashFunction<T> create(int[] fields) {
        return new SimpleHashFunction<>(fields, rowHandler);
    }

    /**
     * Computes a composite hash of a row, given the values of the fields.
     */
    static class SimpleHashFunction<T> implements RowHashFunction<T> {
        private final int[] fields;
        private final RowHandler<T> rowHandler;

        SimpleHashFunction(int[] fields, RowHandler<T> rowHandler) {
            this.fields = fields;
            this.rowHandler = rowHandler;
        }

        @Override
        public int hashOf(T row) {
            int hash = 0;

            for (int idx : fields) {
                hash = 31 * hash + Objects.hashCode(rowHandler.get(idx, row));
            }

            return hash;
        }
    }

    /**
     * Computes a composite hash of a row, given the types and values of the fields.
     */
    static class TypesAwareHashFunction<T> implements RowHashFunction<T> {
        private final int[] fields;
        private final NativeType[] fieldTypes;
        private final RowHandler<T> rowHandler;

        TypesAwareHashFunction(int[] fields, NativeType[] fieldTypes, RowHandler<T> rowHandler) {
            this.fields = fields;
            this.fieldTypes = fieldTypes;
            this.rowHandler = rowHandler;
        }

        @Override
        public int hashOf(T row) {
            HashCalculator hashCalc = new HashCalculator();

            for (int i = 0; i < fields.length; i++) {
                Object value = rowHandler.get(fields[i], row);
                NativeTypeSpec nativeTypeSpec = fieldTypes[i].spec();
                Class<?> storageType = NativeTypeSpec.toClass(nativeTypeSpec, true);

                // TODO Remove this check when https://issues.apache.org/jira/browse/IGNITE-19096 is complete
                if (value == DEFAULT_VALUE_PLACEHOLDER) {
                    var error = format("Placeholder should have been replaced. field: {} nativeTypeSpec: {} row: {} ",
                            fields[i], nativeTypeSpec, rowHandler.toString(row));

                    throw new IllegalArgumentException(error);
                } else {
                    value = TypeUtils.fromInternal(value, storageType);
                }

                ColocationUtils.append(hashCalc, value, fieldTypes[i]);
            }

            return hashCalc.hash();
        }
    }
}
