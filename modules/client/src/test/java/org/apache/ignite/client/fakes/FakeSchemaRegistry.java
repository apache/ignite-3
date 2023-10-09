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

package org.apache.ignite.client.fakes;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.schema.registry.SchemaRegistryException;
import org.apache.ignite.internal.schema.registry.UpgradingRowAdapter;
import org.apache.ignite.internal.schema.row.Row;
import org.jetbrains.annotations.Nullable;

/**
 * Fake schema registry for tests.
 */
public class FakeSchemaRegistry implements SchemaRegistry {
    /** Last registered version. */
    private static volatile int lastVer = 1;

    /** Cached schemas. */
    private final ConcurrentNavigableMap<Integer, SchemaDescriptor> schemaCache = new ConcurrentSkipListMap<>();

    /** Schema store. */
    private final Function<Integer, SchemaDescriptor> history;

    /**
     * Constructor.
     *
     * @param history Schema history.
     */
    public FakeSchemaRegistry(Function<Integer, SchemaDescriptor> history) {
        this.history = history;
    }

    /**
     * Sets the last schema version.
     *
     * @param lastVer Last schema version.
     */
    public static void setLastVer(int lastVer) {
        FakeSchemaRegistry.lastVer = lastVer;
    }

    /** {@inheritDoc} */
    @Override
    public SchemaDescriptor schema(int ver) {
        if (ver == 0) {
            // Use last version (any version may be used) for 0 version, that mean row doens't contain value.
            ver = lastVer;
        }

        SchemaDescriptor desc = schemaCache.get(ver);

        if (desc != null) {
            return desc;
        }

        desc = history.apply(ver);

        if (desc != null) {
            schemaCache.putIfAbsent(ver, desc);

            return desc;
        }

        if (lastVer < ver || ver <= 0) {
            throw new SchemaRegistryException("Incorrect schema version requested: ver=" + ver);
        } else {
            throw new SchemaRegistryException("Failed to find schema: ver=" + ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable SchemaDescriptor schema() {
        return schema(lastVer);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable SchemaDescriptor schemaCached(int ver) {
        return schemaCache.get(ver);
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor waitLatestSchema() {
        return schema();
    }

    /** {@inheritDoc} */
    @Override
    public int lastSchemaVersion() {
        return lastVer;
    }

    /** {@inheritDoc} */
    @Override
    public Row resolve(BinaryRow row, SchemaDescriptor desc) {
        return Row.wrapBinaryRow(desc, row);
    }

    /** {@inheritDoc} */
    @Override
    public Row resolve(BinaryRow row, int targetSchemaVersion) {
        return resolveInternal(row, schema(targetSchemaVersion), false);
    }

    @Override
    public List<Row> resolve(Collection<BinaryRow> rows, int targetSchemaVersion) {
        return resolveCollectionInternal(rows, targetSchemaVersion, false);
    }

    @Override
    public List<Row> resolveKeys(Collection<BinaryRow> keyOnlyRows, int targetSchemaVersion) {
        return resolveCollectionInternal(keyOnlyRows, targetSchemaVersion, true);
    }

    private List<Row> resolveCollectionInternal(Collection<BinaryRow> keyOnlyRows, int targetSchemaVersion, boolean keyOnly) {
        SchemaDescriptor targetSchema = schema(targetSchemaVersion);

        return keyOnlyRows.stream()
                .map(row -> row == null ? null : resolveInternal(row, targetSchema, keyOnly))
                .collect(toList());
    }

    private Row resolveInternal(BinaryRow binaryRow, SchemaDescriptor targetSchema, boolean keyOnly) {
        if (targetSchema == null) {
            throw new SchemaRegistryException("No schema found for the row: schemaVersion=" + binaryRow.schemaVersion());
        }

        if (binaryRow.schemaVersion() == 0 || targetSchema.version() == binaryRow.schemaVersion()) {
            return keyOnly ? Row.wrapKeyOnlyBinaryRow(targetSchema, binaryRow) : Row.wrapBinaryRow(targetSchema, binaryRow);
        }

        SchemaDescriptor rowSchema = schema(binaryRow.schemaVersion());

        ColumnMapper mapping = resolveMapping(targetSchema, rowSchema);

        if (keyOnly) {
            Row row = Row.wrapKeyOnlyBinaryRow(rowSchema, binaryRow);

            return UpgradingRowAdapter.upgradeKeyOnlyRow(targetSchema, mapping, row);
        } else {
            Row row = Row.wrapBinaryRow(rowSchema, binaryRow);

            return UpgradingRowAdapter.upgradeRow(targetSchema, mapping, row);
        }
    }

    private ColumnMapper resolveMapping(SchemaDescriptor targetSchema, SchemaDescriptor rowSchema) {
        assert targetSchema.version() > rowSchema.version()
                : "Target schema version " + targetSchema.version() + " must be higher than row schema version " + rowSchema.version();

        if (targetSchema.version() == rowSchema.version() + 1) {
            return targetSchema.columnMapping();
        }

        ColumnMapper mapping = schema(rowSchema.version() + 1).columnMapping();

        for (int i = rowSchema.version() + 2; i <= targetSchema.version(); i++) {
            mapping = ColumnMapping.mergeMapping(mapping, schema(i));
        }

        return mapping;
    }

    @Override
    public void close() {
        // No-op.
    }
}
