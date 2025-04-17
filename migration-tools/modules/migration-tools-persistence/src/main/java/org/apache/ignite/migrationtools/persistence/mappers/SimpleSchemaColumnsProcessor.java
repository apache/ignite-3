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

package org.apache.ignite.migrationtools.persistence.mappers;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.migrationtools.types.converters.TypeConverterFactory;
import org.apache.ignite3.internal.client.table.ClientSchema;
import org.apache.ignite3.table.Tuple;

/**
 * Provides a mapping layer between Ignite 2 Tuples (created from Binary Objects) and a Ignite 3 table schema.
 */
public class SimpleSchemaColumnsProcessor extends AbstractSchemaColumnsProcessor {

    public SimpleSchemaColumnsProcessor(
            ClientSchema schema,
            Map<String, String> fieldNameForColumn,
            TypeConverterFactory nativeTypeConverters) {
        super(schema, fieldNameForColumn, nativeTypeConverters, false);
    }

    public SimpleSchemaColumnsProcessor(
            ClientSchema schema,
            Map<String, String> fieldNameForColumn,
            TypeConverterFactory nativeTypeConverters,
            boolean packExtraFields) {
        super(schema, fieldNameForColumn, nativeTypeConverters, packExtraFields);
    }

    @Override
    protected Tuple postProcessMappedTuple(Tuple mappedTuple, Collection<String> missingCols, Collection<String> additionalColumns)
            throws RecordAndTableSchemaMismatchException {
        if (missingCols.isEmpty() && additionalColumns.isEmpty()) {
            return mappedTuple;
        } else {
            throw new RecordAndTableSchemaMismatchException(missingCols, additionalColumns);
        }
    }
}
