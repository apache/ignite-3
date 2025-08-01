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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
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
    public ResolvedDependencies resolveDependencies(Iterable<IgniteRel> rels, int catalogVersion) {
        Int2ObjectMap<ExecutableTable> tableMap = new Int2ObjectOpenHashMap<>();
        Int2ObjectMap<ScannableDataSource> dataSources = new Int2ObjectOpenHashMap<>();

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

                resolveTable(catalogVersion, igniteTable.id());

                return super.visit(rel);
            }

            @Override
            public IgniteRel visit(IgniteIndexScan rel) {
                IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

                resolveTable(catalogVersion, igniteTable.id());

                return rel;
            }

            @Override
            public IgniteRel visit(IgniteTableScan rel) {
                IgniteTable igniteTable = rel.getTable().unwrapOrThrow(IgniteTable.class);

                resolveTable(catalogVersion, igniteTable.id());

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
                if (distribution.isTableDistribution()) {
                    int tableId = distribution.tableId();

                    resolveTable(catalogVersion, tableId);
                }
            }

            private void resolveTable(int catalogVersion, int tableId) {
                tableMap.computeIfAbsent(tableId, (id) -> registry.getTable(catalogVersion, tableId));
            }
        };

        for (IgniteRel rel : rels) {
            shuttle.visit(rel);
        }

        return new ResolvedDependencies(tableMap, dataSources);
    }
}
