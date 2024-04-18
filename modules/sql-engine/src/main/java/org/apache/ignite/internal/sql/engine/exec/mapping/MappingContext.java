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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.sql.engine.exec.mapping.smallcluster.SmallClusterFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * A context that encloses information necessary during mapping.
 */
class MappingContext {
    private final String localNode;
    private final List<String> nodes;

    private final ExecutionTargetFactory targetFactory;

    private RelOptCluster cluster;

    MappingContext(String localNode, List<String> nodes) {
        this.localNode = localNode;
        this.nodes = nodes;

        if (nodes.size() > 64) {
            throw new UnsupportedOperationException("https://issues.apache.org/jira/browse/IGNITE-20503");
        }

        this.targetFactory = new SmallClusterFactory(nodes);
    }

    public RelOptCluster cluster() {
        if (cluster == null) {
            cluster = Commons.cluster();
        }

        return cluster;
    }

    public String localNode() {
        return localNode;
    }

    public List<String> nodes() {
        return nodes;
    }

    public ExecutionTargetFactory targetFactory() {
        return targetFactory;
    }
}
