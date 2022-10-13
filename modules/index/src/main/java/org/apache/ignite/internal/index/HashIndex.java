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

package org.apache.ignite.internal.index;

import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.PublisherAdapter;

/**
 * An object that represents a hash index.
 */
public class HashIndex implements Index<IndexDescriptor> {
    private final UUID id;
    private final InternalTable table;
    private final IndexDescriptor descriptor;
    private final SchemaRegistry schemaRegistry;

    /**
     * Constructs the index.
     *
     * @param id An identifier of the index.
     * @param table A table this index relates to.
     * @param descriptor A descriptor of the index.
     */
    public HashIndex(UUID id, TableImpl table, IndexDescriptor descriptor) {
        this.id = Objects.requireNonNull(id, "id");
        this.table = Objects.requireNonNull(table.internalTable(), "table");
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");

        schemaRegistry = table.schemaView();
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public UUID tableId() {
        return table.tableId();
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return descriptor.name();
    }

    /** {@inheritDoc} */
    @Override
    public IndexDescriptor descriptor() {
        return descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryTuple> scan(int partId, InternalTransaction tx, BinaryTuple key, BitSet columns) {
        return new PublisherAdapter<>(
                table.scan(partId, tx, id, key, columns),
                this::convertToTuple
        );
    }

    // TODO: fix row conversion, apply projection, upgrade row version if needed.
    private BinaryTuple convertToTuple(BinaryRow row) {
        SchemaDescriptor schemaDesc = schemaRegistry.schema(row.schemaVersion());

        return BinaryConverter.forRow(schemaDesc).toTuple(row);
    }
}
