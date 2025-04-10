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
