/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
