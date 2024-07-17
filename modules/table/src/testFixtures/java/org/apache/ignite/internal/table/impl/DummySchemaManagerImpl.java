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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;

import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;

/** Dummy schema manager for tests. */
public class DummySchemaManagerImpl implements SchemaRegistry {
    private final NavigableMap<Integer, SchemaDescriptor> schemaByVersion;

    /** Constructor. */
    public DummySchemaManagerImpl(SchemaDescriptor... schemas) {
        assert !nullOrEmpty(schemas);

        schemaByVersion = Stream.of(schemas).collect(toMap(SchemaDescriptor::version, identity(), (o, o2) -> o2, TreeMap::new));
    }

    @Override
    public SchemaDescriptor lastKnownSchema() {
        return schemaByVersion.lastEntry().getValue();
    }

    @Override
    public SchemaDescriptor schema(int version) {
        assert schemaByVersion.containsKey(version) : version;

        return schemaByVersion.get(version);
    }

    @Override
    public CompletableFuture<SchemaDescriptor> schemaAsync(int version) {
        return completedFuture(schema(version));
    }

    @Override
    public int lastKnownSchemaVersion() {
        return lastKnownSchema().version();
    }

    @Override
    public Row resolve(BinaryRow row, SchemaDescriptor desc) {
        return Row.wrapBinaryRow(desc, row);
    }

    @Override
    public Row resolve(BinaryRow row, int targetSchemaVersion) {
        assert targetSchemaVersion == row.schemaVersion() : "row=" + row.schemaVersion() + ", target=" + targetSchemaVersion;

        return Row.wrapBinaryRow(schema(targetSchemaVersion), row);
    }

    @Override
    public List<Row> resolve(Collection<BinaryRow> rows, int targetSchemaVersion) {
        return rows.stream()
                .map(row -> row == null ? null : resolve(row, targetSchemaVersion))
                .collect(toList());
    }

    @Override
    public List<Row> resolveKeys(Collection<BinaryRow> keyOnlyRows, int targetSchemaVersion) {
        for (BinaryRow row : keyOnlyRows) {
            assert row == null || row.schemaVersion() == targetSchemaVersion;
        }

        return keyOnlyRows.stream()
                .map(row -> row == null ? null : Row.wrapKeyOnlyBinaryRow(schema(row.schemaVersion()), row))
                .collect(toList());
    }

    @Override
    public void close() {
        // No-op.
    }
}
