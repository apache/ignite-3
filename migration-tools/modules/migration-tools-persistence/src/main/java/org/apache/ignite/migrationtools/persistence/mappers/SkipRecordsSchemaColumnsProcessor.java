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

import java.util.Map;
import org.apache.ignite.migrationtools.types.converters.TypeConverterFactory;
import org.apache.ignite3.internal.client.table.ClientSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a mapping layer between Ignite 2 Tuples (created from Binary Objects) and a Ignite 3 table schema.
 */
public class SkipRecordsSchemaColumnsProcessor extends SimpleSchemaColumnsProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SkipRecordsSchemaColumnsProcessor.class);

    private long skippedRecords;

    public SkipRecordsSchemaColumnsProcessor(
            ClientSchema schema,
            Map<String, String> fieldNameForColumn,
            TypeConverterFactory nativeTypeConverters) {
        super(schema, fieldNameForColumn, nativeTypeConverters);
        this.skippedRecords = 0;
    }

    @Override
    public SchemaColumnProcessorStats getStats() {
        var superStats = super.getStats();
        return new SkippedRecordsStats(superStats.getProcessedElements(), this.skippedRecords);
    }

    @Override
    public void onError(Throwable ex) {
        if (ex instanceof RecordAndTableSchemaMismatchException) {
            LOGGER.warn("Error matching cache record to table schema. Skipping entire record.", ex);
            this.skippedRecords++;
            subscription.request(1);
        } else {
            super.onError(ex);
        }
    }

    /** SkippedRecordsStats. */
    public static class SkippedRecordsStats extends SchemaColumnProcessorStats {

        private long numSkippedRecords;

        private SkippedRecordsStats() {
            super();
        }

        public SkippedRecordsStats(long processedElements, long numSkippedRecords) {
            super(processedElements);
            this.numSkippedRecords = numSkippedRecords;
        }

        public long getNumSkippedRecords() {
            return numSkippedRecords;
        }

        @Override
        public String toString() {
            return "SkippedRecordsStats{numProcessedRecords=" + this.getProcessedElements() + " ,numSkippedRecords=" + numSkippedRecords
                    + '}';
        }
    }
}
