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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.schema.registry.SchemaRegistryException;
import org.apache.ignite.internal.schema.row.Row;

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
     * Gets schema descriptor for the latest version known locally. It might be not the last schema version cluster-wide.
     *
     * <p>No schema synchronization guarantees are provided (that is, one cannot assume that the returned
     * schema version corresponds to the current moment from the point of view of other nodes).
     *
     * <p>This method never blocks.
     */
    SchemaDescriptor lastKnownSchema();

    /**
     * Gets schema descriptor for given version or throws an exception if the given version is not available.
     * If 0 is passed as a version, this returns the latest known version.
     *
     * <p>This method never blocks.
     *
     * @param version Schema version to get descriptor for (if 0, then the latest known version is requested).
     * @return Schema descriptor of given version (or latest known version if version 0 is requested).
     * @throws SchemaRegistryException If no schema found for given version.
     */
    SchemaDescriptor schema(int version) throws SchemaRegistryException;

    /**
     * Gets schema descriptor for given version asynchronously.
     *
     * @param version Schema version to get descriptor for (0 is not a valid argument).
     * @return Future that will complete when a schema descriptor of given version becomes available.
     */
    CompletableFuture<SchemaDescriptor> schemaAsync(int version);

    /**
     * Returns last schema version known locally. It might be not the last schema version cluster-wide.
     *
     * <p>No schema synchronization guarantees are provided (that is, one cannot assume that the returned
     * schema version corresponds to the current moment from the point of view of other nodes).
     *
     * <p>This method never blocks.
     */
    int lastKnownSchemaVersion();

    /**
     * Resolve binary row against given schema ({@code desc}). The row schema version must be lower or equal to the version
     * of {@code desc}. If the schema versions are not equal, the row will be upgraded to {@code desc}.
     *
     * <p>This method never blocks.
     *
     * @param row  Binary row.
     * @param desc Schema descriptor.
     * @return Schema-aware row.
     */
    Row resolve(BinaryRow row, SchemaDescriptor desc);

    /**
     * Resolve row against a given schema. The row schema version must be lower or equal to the target version.
     * If the schema versions are not equal, the row will be upgraded to the target schema.
     *
     * <p>This method never blocks.
     *
     * @param row Binary row.
     * @param targetSchemaVersion Schema version to which the row must be resolved.
     * @return Schema-aware row.
     */
    Row resolve(BinaryRow row, int targetSchemaVersion);

    /**
     * Resolves batch of binary rows against a given schema. Each row schema version must be lower or equal to the target version.
     * If the schema versions are not equal, the row will be upgraded to the target schema.
     *
     * <p>This method never blocks.
     *
     * @param rows Binary rows.
     * @param targetSchemaVersion Schema version to which the rows must be resolved.
     * @return Schema-aware rows. Contains {@code null}s at the same positions as in {@code rows}.
     */
    List<Row> resolve(Collection<BinaryRow> rows, int targetSchemaVersion);

    /**
     * Resolves batch of binary rows, that only contain the key component, against a given schema.
     * Each row schema version must be lower or equal to the target version.
     * If the schema versions are not equal, the row will be upgraded to the target schema.
     *
     * <p>This method never blocks.
     *
     * @param keyOnlyRows Binary rows that only contain the key component.
     * @param targetSchemaVersion Schema version to which the rows must be resolved.
     * @return Schema-aware rows. Contains {@code null}s at the same positions as in {@code keyOnlyRows}.
     */
    List<Row> resolveKeys(Collection<BinaryRow> keyOnlyRows, int targetSchemaVersion);

    /**
     * Closes the registry freeing any resources it holds.
     */
    @Override
    void close();
}
