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

package org.apache.ignite.internal.client.sql;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.client.PartitionMapping;
import org.apache.ignite.internal.client.table.ClientColumn;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.util.HashCalculator;
import org.jspecify.annotations.Nullable;

/**
 * Provider that computes {@link PartitionMapping partition mapping} based on a
 * {@link ClientPartitionAwarenessMetadata partition awareness meta} and values of dynamic parameters.
 *
 * <p>The rationale behind this class is that we need to cache table schema and assignment somewhere, so we can reuse it
 * rather than request them every time from server on every query execution.
 *
 * <p>Assignments are cached temporarily, and updated after certain time. See {@link ClientTable#getPartitionAssignment()} for details.
 */
public class PartitionMappingProvider {
    private final int tableId;
    private final Supplier<@Nullable ClientSchema> schemaProvider;
    private final Supplier<@Nullable List<String>> assignmentsProvider;
    private final int[] indexes;
    private final int[] hash;
    private final ClientDirectTxMode directTxMode;

    static PartitionMappingProvider create(
            ClientTable table,
            ClientPartitionAwarenessMetadata meta,
            Consumer<Throwable> onErrorCallback
    ) {
        assert table.tableId() == meta.tableId();

        CompletableFuture<ClientSchema> schemaFuture = table.getLatestSchema();

        return new PartitionMappingProvider(
                meta.tableId(),
                meta.indexes(),
                meta.hash(),
                meta.directTxMode(),
                () -> nullOnError(schemaFuture, onErrorCallback).getNow(null),
                () -> nullOnError(table.getPartitionAssignment(), onErrorCallback).getNow(null)
        );
    }

    private static <T> CompletableFuture<@Nullable T> nullOnError(CompletableFuture<T> future, Consumer<Throwable> onErrorCallback) {
        return future.handle((result, throwable) -> {
            if (throwable != null) {
                onErrorCallback.accept(throwable);

                return null;
            }

            return result;
        });
    }

    private PartitionMappingProvider(
            int tableId,
            int[] indexes,
            int[] hash,
            ClientDirectTxMode directTxMode,
            Supplier<@Nullable ClientSchema> schemaProvider,
            Supplier<@Nullable List<String>> assignmentsProvider
    ) {
        this.tableId = tableId;
        this.indexes = indexes;
        this.hash = hash;
        this.directTxMode = directTxMode;
        this.schemaProvider = schemaProvider;
        this.assignmentsProvider = assignmentsProvider;
    }

    public int tableId() {
        return tableId;
    }

    public int[] indexes() {
        return indexes;
    }

    public int[] hash() {
        return hash;
    }

    public ClientDirectTxMode directTxMode() {
        return directTxMode;
    }

    public boolean ready() {
        return schemaProvider.get() != null && assignmentsProvider.get() != null;
    }

    /**
     * Computes {@link PartitionMapping} based on a given dynamic parameters.
     *
     * <p>Mapping is calculated with regard to colocation info derived from table's {@link ClientSchema schema} and partition assignments.
     * Hence, if either of this is not yet loaded from the server, mapping cannot be computed, thus {@code null} will be returned.
     *
     * @param params Values of dynamic parameters.
     * @return A mapping to use to route query if all the information required is available now, returns {@code null} otherwise.
     */
    public @Nullable PartitionMapping get(Object[] params) {
        ClientSchema schema = schemaProvider.get();
        List<String> assignments = assignmentsProvider.get();

        if (schema == null || assignments == null) {
            // Without schema we don't have all the information required to properly
            // compute colocation hash. Namely, type info like scale and precision is missing.
            return null;
        }

        HashCalculator calculator = new HashCalculator();
        ClientColumn[] colocationColumns = schema.colocationColumns();

        if (colocationColumns.length != indexes.length) {
            assert false;

            return null;
        }

        for (int i = 0; i < colocationColumns.length; i++) {
            int idx = indexes[i];

            if (idx >= params.length) {
                // This may be possible if the same query is executed with different number of dynamic parameters.
                // Such query won't pass validation anyway since number of parameters passed must be strongly equal
                // to the number of parameters specified in a query string. It doesn't matter which node will send
                // an exception.
                return null;
            }

            if (idx >= 0) {
                ClientColumn column = colocationColumns[i];

                calculator.append(params[indexes[i]], column.scale(), column.precision());
            } else {
                calculator.combine(hash[-(idx + 1)]);
            }
        }

        int colocationHash = calculator.hash();
        int part = Math.abs(colocationHash % assignments.size());

        String node = assignments.get(part);
        if (node == null) {
            return null; // Mapping is incomplete.
        }

        return new PartitionMapping(tableId, node, part);
    }
}
