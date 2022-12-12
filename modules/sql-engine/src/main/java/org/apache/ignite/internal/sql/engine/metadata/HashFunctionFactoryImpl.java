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

package org.apache.ignite.internal.sql.engine.metadata;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.ToIntFunction;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.util.ColocationUtils;
import org.apache.ignite.internal.util.HashCalculator;

/**
 * Factory for creating a function to calculate the hash of the specified fields of a row.
 */
public class HashFunctionFactoryImpl<T> implements HashFunctionFactory<T> {
    private final SqlSchemaManager sqlSchemaManager;
    private final RowHandler<T> rowHandler;

    public HashFunctionFactoryImpl(SqlSchemaManager sqlSchemaManager, RowHandler<T> rowHandler) {
        this.sqlSchemaManager = sqlSchemaManager;
        this.rowHandler = rowHandler;
    }

    @Override
    public ToIntFunction<T> create(boolean typesAware, int[] fields, UUID tableId) {
        assert !typesAware || tableId != null;

        return typesAware
                ? new TypesAwareHashFunction(tableId, fields)
                : new SimpleHashFunction(fields);
    }

    /**
     * Computes a composite hash of a row, given the values of the fields.
     */
    private class SimpleHashFunction implements ToIntFunction<T> {
        private final int[] fields;

        private SimpleHashFunction(int[] fields) {
            this.fields = fields;
        }

        @Override
        public int applyAsInt(T row) {
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
    private class TypesAwareHashFunction implements ToIntFunction<T> {
        private final int[] fields;
        private final NativeType[] fieldTypes;

        private TypesAwareHashFunction(UUID tableId, int[] fields) {
            this.fieldTypes = fieldTypes(tableId, fields.length);
            this.fields = fields;
        }

        private NativeType[] fieldTypes(UUID tableId, int fieldCnt) {
            NativeType[] colTypes = new NativeType[fieldCnt];
            TableDescriptor tblDesc = sqlSchemaManager.tableById(tableId, -1).descriptor();
            List<Integer> colocationColumns = tblDesc.distribution().getKeys();

            assert colocationColumns.size() == fieldCnt : "fieldsCount=" + fieldCnt + ", colocationColumns=" + colocationColumns;

            for (int i = 0; i < fieldCnt; i++) {
                ColumnDescriptor colDesc = tblDesc.columnDescriptor(colocationColumns.get(i));

                colTypes[i] = colDesc.physicalType();
            }

            return colTypes;
        }

        @Override
        public int applyAsInt(T row) {
            HashCalculator hashCalc = new HashCalculator();

            for (int i = 0; i < fields.length; i++) {
                Object obj = rowHandler.get(fields[i], row);

                ColocationUtils.append(hashCalc, obj, fieldTypes[i]);
            }

            return hashCalc.hash();
        }
    }
}
