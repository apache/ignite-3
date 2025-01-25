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

package org.apache.ignite.internal.sql.engine.framework;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.AssignmentsProvider;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * A test cluster object.
 *
 * <p>This is convenient holder of collection of nodes which provides methods for centralised
 * accessing and management.
 *
 * <p>NB: do not forget to {@link #start()} cluster before use, and {@link #stop()} the cluster after.
 */
public class TestCluster implements LifecycleAware {
    private final Map<String, TestNode> nodeByName;
    private final List<LifecycleAware> components;
    private final Runnable initClosure;
    private final RunnableX stopClosure;
    private final CatalogManager catalogManager;
    private final ConcurrentMap<String, ScannableTable> dataProvidersByTableName;
    private final ConcurrentMap<String, UpdatableTable> updatableTablesByName;
    private final ConcurrentMap<String, AssignmentsProvider> assignmentsProvidersByTableName;
    private final ConcurrentMap<String, Long> tablesSize;

    TestCluster(
            ConcurrentMap<String, Long> tablesSize,
            ConcurrentMap<String, ScannableTable> dataProvidersByTableName,
            ConcurrentMap<String, UpdatableTable> updatableTablesByName,
            ConcurrentMap<String, AssignmentsProvider> assignmentsProvidersByTableName,
            Map<String, TestNode> nodeByName,
            CatalogManager catalogManager,
            PrepareService prepareService,
            ClockWaiter clockWaiter,
            Runnable initClosure,
            RunnableX stopClosure
    ) {
        this.tablesSize = tablesSize;
        this.dataProvidersByTableName = dataProvidersByTableName;
        this.updatableTablesByName = updatableTablesByName;
        this.assignmentsProvidersByTableName = assignmentsProvidersByTableName;
        this.nodeByName = nodeByName;
        this.components = List.of(
                new ComponentToLifecycleAwareAdaptor(catalogManager),
                prepareService,
                new ComponentToLifecycleAwareAdaptor(clockWaiter)
        );
        this.initClosure = initClosure;
        this.catalogManager = catalogManager;
        this.stopClosure = stopClosure;
    }

    public CatalogManager catalogManager() {
        return catalogManager;
    }

    /**
     * Returns the node for the given name, if exists.
     *
     * @param name A name of the node of interest.
     * @return A test node or {@code null} if there is no node with such name.
     */
    public TestNode node(String name) {
        return nodeByName.get(name);
    }

    @Override
    public void start() {
        components.forEach(LifecycleAware::start);

        nodeByName.values().forEach(TestNode::start);

        nodeByName.values().iterator().next().initSchema(
                "CREATE TABLE blackhole (x INT PRIMARY KEY)"
        );

        initClosure.run();
    }

    @Override
    public void stop() throws Exception {
        List<AutoCloseable> closeables = Stream.concat(
                        components.stream(),
                        nodeByName.values().stream()
                )
                .map(node -> ((AutoCloseable) node::stop))
                .collect(Collectors.toList());

        Collections.reverse(closeables);
        IgniteUtils.closeAll(closeables);

        try {
            stopClosure.run();
        } catch (Throwable t) {
            throw new Exception(t);
        }
    }

    public void setAssignmentsProvider(String tableName, AssignmentsProvider assignmentsProvider) {
        assignmentsProvidersByTableName.put(tableName, assignmentsProvider);
    }

    public void setDataProvider(String tableName, ScannableTable dataProvider) {
        dataProvidersByTableName.put(tableName, dataProvider);
    }

    public void setUpdatableTable(String tableName, UpdatableTable table) {
        updatableTablesByName.put(tableName, table);
    }

    public void setTableSize(String name, long size) {
        tablesSize.put(name, size);
    }

    private static class ComponentToLifecycleAwareAdaptor implements LifecycleAware {
        private final IgniteComponent component;

        ComponentToLifecycleAwareAdaptor(IgniteComponent component) {
            this.component = component;
        }

        @Override
        public void start() {
            assertThat(component.startAsync(new ComponentContext()), willCompleteSuccessfully());
        }

        @Override
        public void stop() {
            assertThat(component.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }
    }
}
