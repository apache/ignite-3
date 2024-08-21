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

package org.apache.ignite.internal.restart;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;

/**
 * Reference to {@link IgniteTables} under a swappable {@link Ignite} instance. When a restart happens, this switches to the new Ignite
 * instance.
 *
 * <p>API operations on this are linearized wrt node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
class RestartProofIgniteTables implements IgniteTables, Wrapper {
    private final IgniteAttachmentLock attachmentLock;

    RestartProofIgniteTables(IgniteAttachmentLock attachmentLock) {
        this.attachmentLock = attachmentLock;
    }

    @Override
    public List<Table> tables() {
        return attachmentLock.attached(ignite -> {
            List<Table> tables = ignite.tables().tables();
            return wrapTables(tables, ignite);
        });
    }

    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        return attachmentLock.attachedAsync(ignite ->
                ignite.tables().tablesAsync()
                        .thenApply(tables -> wrapTables(tables, ignite))
        );
    }

    @Override
    public Table table(String name) {
        return attachmentLock.attached(ignite -> {
            Table table = ignite.tables().table(name);
            return wrapTable(table, ignite);
        });
    }

    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        return attachmentLock.attachedAsync(ignite ->
                ignite.tables().tableAsync(name)
                        .thenApply(table -> wrapTable(table, ignite))
        );
    }

    private @Nullable Table wrapTable(Table table, Ignite ignite) {
        if (table == null) {
            return null;
        }

        return new RestartProofTable(attachmentLock, ignite, RestartProofTable.tableId(table));
    }

    private List<Table> wrapTables(List<Table> tables, Ignite ignite) {
        return tables.stream().map(table -> wrapTable(table, ignite)).collect(toList());
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attachmentLock.attached(ignite -> Wrappers.unwrap(ignite.tables(), classToUnwrap));
    }
}
