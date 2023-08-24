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

package org.apache.ignite.internal.table.impl;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy schema manager for tests.
 */
public class DummySchemaManagerImpl implements SchemaRegistry {
    /** Schema. */
    private final SchemaDescriptor schema;

    /**
     * Constructor.
     *
     * @param schema Schema descriptor.
     */
    public DummySchemaManagerImpl(SchemaDescriptor schema) {
        assert schema != null;

        this.schema = schema;
    }

    /** {@inheritDoc} */
    @Override
    public SchemaDescriptor schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override
    public SchemaDescriptor schema(int ver) {
        assert ver >= 0;

        assert schema.version() == ver;

        return schema;
    }

    @Override
    public @Nullable SchemaDescriptor schemaCached(int ver) {
        assert schema.version() == ver;

        return schema;
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor waitLatestSchema() {
        return schema();
    }

    /** {@inheritDoc} */
    @Override
    public int lastSchemaVersion() {
        return schema.version();
    }

    /** {@inheritDoc} */
    @Override
    public Row resolve(BinaryRow row, SchemaDescriptor desc) {
        return Row.wrapBinaryRow(desc, row);
    }

    /** {@inheritDoc} */
    @Override
    public Row resolve(BinaryRow row) {
        assert row.schemaVersion() == schema.version() || row.schemaVersion() == 0;

        return Row.wrapBinaryRow(schema, row);
    }

    @Override
    public List<Row> resolve(Collection<BinaryRow> rows) {
        return rows.stream()
                .map(row -> row == null ? null : Row.wrapBinaryRow(schema(row.schemaVersion()), row))
                .collect(toList());
    }

    @Override
    public List<Row> resolveKeys(Collection<BinaryRow> keyOnlyRows) {
        return keyOnlyRows.stream()
                .map(row -> row == null ? null : Row.wrapKeyOnlyBinaryRow(schema(row.schemaVersion()), row))
                .collect(toList());
    }

    @Override
    public void close() {
        // No-op.
    }
}
