/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
