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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
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

    TestCluster(
            Map<String, TestNode> nodeByName,
            List<LifecycleAware> components,
            Runnable initClosure
    ) {
        this.nodeByName = nodeByName;
        this.components = components;
        this.initClosure = initClosure;
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

    /** {@inheritDoc} */
    @Override
    public void start() {
        components.forEach(LifecycleAware::start);

        nodeByName.values().forEach(TestNode::start);


        initClosure.run();
    }

    /** {@inheritDoc} */
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
    }
}
