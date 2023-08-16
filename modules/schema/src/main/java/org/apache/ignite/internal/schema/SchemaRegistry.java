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

package org.apache.ignite.internal.schema;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.schema.registry.SchemaRegistryException;
import org.apache.ignite.internal.schema.row.Row;
import org.jetbrains.annotations.Nullable;

/**
 * Table schema registry interface.
 *
 * <p>Schemas itself MUST be registered in a version ascending order incrementing by {@code 1} with NO gaps, otherwise an exception will be
 * thrown. The version numbering starts from the {@code 1}.
 *
 * <p>After some table maintenance process some first versions may become outdated and can be safely cleaned up if the process guarantees
 * the table no longer has a data of these versions.
 *
 * @implSpec The changes in between two arbitrary actual versions MUST NOT be lost. Thus, schema versions can only be removed from the
 *      beginning.
 * @implSpec Initial schema history MAY be registered without the first outdated versions that could be cleaned up earlier.
 */
public interface SchemaRegistry extends ManuallyCloseable {
    /**
     * Gets schema descriptor for the latest version if initialized.
     *
     * @return Schema descriptor if initialized, {@code null} otherwise.
     */
    SchemaDescriptor schema();

    /**
     * Gets schema descriptor for given version.
     *
     * @param ver Schema version to get descriptor for.
     * @return Schema descriptor of given version.
     * @throws SchemaRegistryException If no schema found for given version.
     */
    SchemaDescriptor schema(int ver) throws SchemaRegistryException;

    /**
     * Gets cached schema descriptor for given version.
     *
     * @param ver Schema version to get descriptor for.
     * @return Schema descriptor of given version or {@code null} if missed in cache.
     */
    @Nullable SchemaDescriptor schemaCached(int ver);

    /**
     * Gets schema descriptor for the latest version in cluster.
     *
     * @return Schema descriptor if initialized, {@code null} otherwise.
     */
    SchemaDescriptor waitLatestSchema();

    /**
     * Get last registered schema version.
     */
    int lastSchemaVersion();

    /**
     * Resolve binary row against given schema.
     *
     * @param row  Binary row.
     * @param desc Schema descriptor.
     * @return Schema-aware row.
     */
    Row resolve(BinaryRow row, SchemaDescriptor desc);

    /**
     * Resolve row against the latest schema.
     *
     * @param row Binary row.
     * @return Schema-aware row.
     */
    Row resolve(BinaryRow row);

    /**
     * Resolves batch of binary rows against the latest schema.
     *
     * @param rows Binary rows.
     * @return Schema-aware rows. Contains {@code null} at the same positions as in {@code rows}.
     */
    List<Row> resolve(Collection<BinaryRow> rows);

    /**
     * Resolves batch of binary rows, that only contain the key component, against the latest schema.
     *
     * @param keyOnlyRows Binary rows that only contain the key component.
     * @return Schema-aware rows. Contains {@code null} at the same positions as in {@code keyOnlyRows}.
     */
    List<Row> resolveKeys(Collection<BinaryRow> keyOnlyRows);

    /**
     * Closes the registry freeing any resources it holds.
     */
    @Override
    void close();
}
