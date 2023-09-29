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
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction.AffinityDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;

/**
 * Acquires dependencies from appropriate component managers.
 */
public class ExecutionDependencyResolverImpl implements ExecutionDependencyResolver {

    private final ExecutableTableRegistry registry;
    private final ScannableDataSourceProvider dataSourceProvider;

    public ExecutionDependencyResolverImpl(
            ExecutableTableRegistry registry,
            ScannableDataSourceProvider dataSourceProvider
    ) {
        this.registry = registry;
        this.dataSourceProvider = dataSourceProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<ResolvedDependencies> resolveDependencies(Iterable<IgniteRel> rels, IgniteSchema schema) {
        Map<Integer, CompletableFuture<ExecutableTable>> tableMap = new HashMap<>();
        Map<Integer, ScannableDataSource> dataSources = new HashMap<>();

        IgniteRelShuttle shuttle = new IgniteRelShuttle() {
            @Override
            public IgniteRel visit(IgniteSender rel) {
                IgniteDistribution distribution = TraitUtils.distribution(rel);

                resolveDistributionFunction(distribution);

                return super.visit(rel);
            }

            @Override
            public IgniteRel visit(IgniteTrimExchange rel) {
                IgniteDistribution distribution = TraitUtils.distribution(rel);

                resolveDistributionFunction(distribution);

                return super.visit(rel);
            }

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

            @Override
            public IgniteRel visit(IgniteSystemViewScan rel) {
                IgniteSystemView view = rel.getTable().unwrap(IgniteSystemView.class);

                assert view != null;

                dataSources.put(view.id(), dataSourceProvider.forSystemView(view));

                return rel;
            }

            private void resolveDistributionFunction(IgniteDistribution distribution) {
                DistributionFunction function = distribution.function();

                if (function.affinity()) {
                    int tableId = ((AffinityDistribution) function).tableId();

                    resolveTable(schema.getTable(tableId));
                }
            }

            private void resolveTable(IgniteTable igniteTable) {
                int tableId = igniteTable.id();
                TableDescriptor tableDescriptor = igniteTable.descriptor();
                int tableVersion = igniteTable.version();

                tableMap.computeIfAbsent(tableId, (id) -> registry.getTable(tableId, tableVersion, tableDescriptor));
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

                    return new ResolvedDependencies(map, dataSources);
                });
    }
}
