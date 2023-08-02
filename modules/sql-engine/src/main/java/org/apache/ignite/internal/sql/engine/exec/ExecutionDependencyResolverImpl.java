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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;

/**
 * Acquires dependencies from appropriate component managers.
 */
public class ExecutionDependencyResolverImpl implements ExecutionDependencyResolver {

    private final ExecutableTableRegistry registry;

    public ExecutionDependencyResolverImpl(ExecutableTableRegistry registry) {
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<ResolvedDependencies> resolveDependencies(Iterable<IgniteRel> rels, long schemaVersion) {
        Map<Integer, CompletableFuture<ExecutableTable>> tableMap = new HashMap<>();

        IgniteRelShuttle shuttle = new IgniteRelShuttle() {
            @Override
            public IgniteRel visit(IgniteTableModify rel) {
                IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

                resolveTable(igniteTable);

                return super.visit(rel);
            }

            @Override
            public IgniteRel visit(IgniteIndexScan rel) {
                IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

                resolveTable(igniteTable);

                return rel;
            }

            @Override
            public IgniteRel visit(IgniteTableScan rel) {
                IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

                resolveTable(igniteTable);

                return rel;
            }

            private void resolveTable(IgniteTable igniteTable) {
                int tableId = igniteTable.id();
                TableDescriptor tableDescriptor = igniteTable.descriptor();
                //TODO IGNITE-19499 Use id instead of name.
                String tableName = igniteTable.name();

                tableMap.computeIfAbsent(tableId, (id) -> registry.getTable(tableId, tableName, tableDescriptor));
            }
        };

        for (IgniteRel rel : rels) {
            shuttle.visit(rel);
        }

        List<CompletableFuture<ExecutableTable>> fs = new ArrayList<>(tableMap.values());

        return CompletableFuture.allOf(fs.toArray(new CompletableFuture<?>[0]))
                .thenApply(r -> {
                    Map<Integer, ExecutableTable> map = tableMap.entrySet()
                            .stream()
                            .map(e -> Map.entry(e.getKey(), e.getValue().join()))
                            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

                    return new ResolvedDependencies(map);
                });
    }
}
