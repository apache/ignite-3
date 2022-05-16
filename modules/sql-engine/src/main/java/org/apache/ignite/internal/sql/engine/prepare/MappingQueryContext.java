/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.prepare;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.ignite.internal.sql.engine.metadata.IgniteMetadata;
import org.apache.ignite.internal.sql.engine.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Query mapping context.
 */
public class MappingQueryContext {
    private final String locNodeId;

    private RelOptCluster cluster;

    /**
     * Constructor.
     *
     * @param locNodeId Local node identifier.
     */
    public MappingQueryContext(String locNodeId) {
        this.locNodeId = locNodeId;
    }

    /** Creates a cluster. */
    RelOptCluster cluster() {
        if (cluster == null) {
            cluster = RelOptCluster.create(Commons.cluster().getPlanner(), Commons.cluster().getRexBuilder());
            cluster.setMetadataProvider(new CachingRelMetadataProvider(IgniteMetadata.METADATA_PROVIDER,
                    Commons.cluster().getPlanner()));
            cluster.setMetadataQuerySupplier(RelMetadataQueryEx::create);
        }

        return cluster;
    }

    public String localNodeId() {
        return locNodeId;
    }
}
