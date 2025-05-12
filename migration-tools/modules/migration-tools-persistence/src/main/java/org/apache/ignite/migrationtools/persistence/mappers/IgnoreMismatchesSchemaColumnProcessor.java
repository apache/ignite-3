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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.migrationtools.types.converters.TypeConverterFactory;
import org.apache.ignite3.internal.client.table.ClientSchema;
import org.apache.ignite3.table.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a mapping layer between Ignite 2 Tuples (created from Binary Objects) and a Ignite 3 table schema.
 */
public class IgnoreMismatchesSchemaColumnProcessor extends AbstractSchemaColumnsProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgnoreMismatchesSchemaColumnProcessor.class);

    private Map<String, Long> droppedColumns;

    public IgnoreMismatchesSchemaColumnProcessor(
            ClientSchema schema,
            Map<String, String> fieldNameForColumn,
            TypeConverterFactory nativeTypeConverters) {
        super(schema, fieldNameForColumn, nativeTypeConverters, false);
        this.droppedColumns = new HashMap<>();
    }

    @Override
    protected Tuple postProcessMappedTuple(Tuple mappedTuple, Collection<String> missingCols, Collection<String> additionalColumns)
            throws RecordAndTableSchemaMismatchException {
        if (!missingCols.isEmpty()) {
            throw new RecordAndTableSchemaMismatchException(missingCols, additionalColumns);
        }

        if (!additionalColumns.isEmpty()) {
            LOGGER.warn("Found additional columns in tuple: {} Ignoring..", additionalColumns);
            for (String colName : additionalColumns) {
                this.droppedColumns.compute(colName, (k, prev) -> ((prev != null) ? prev : 0) + 1);
            }
        }

        return mappedTuple;
    }

    @Override
    public SchemaColumnProcessorStats getStats() {
        var superStats = super.getStats();
        return new IgnoredColumnsStats(superStats.getProcessedElements(), this.droppedColumns);
    }

    /** IgnoredColumnsStats. */
    public static class IgnoredColumnsStats extends SchemaColumnProcessorStats {

        private Map<String, Long> droppedColumns;

        private IgnoredColumnsStats() {
            super();
        }

        public IgnoredColumnsStats(long processedElements, Map<String, Long> droppedColumns) {
            super(processedElements);
            this.droppedColumns = Collections.unmodifiableMap(droppedColumns);
        }

        public Map<String, Long> getDroppedColumns() {
            return droppedColumns;
        }

        @Override
        public String toString() {
            return "Stats{processedElements=" + this.getProcessedElements() + ", droppedColumns=" + droppedColumns + '}';
        }
    }
}
