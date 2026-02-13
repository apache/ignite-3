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

package org.apache.ignite.internal.sql.engine.prepare;

import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.explain.ExplainUtils;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Regular query or DML.
 */
public class MultiStepPlan implements ExplainablePlan {

    private final PlanId id;

    private final SqlQueryType type;

    private final ResultSetMetadata meta;

    private final IgniteRel root;

    private final ParameterMetadata parameterMetadata;

    private final int catalogVersion;

    private final @Nullable QueryPlan fastPlan;

    private final int numSources;

    private final PartitionAwarenessMetadata partitionAwarenessMetadata;

    private final PartitionPruningMetadata partitionPruningMetadata;

    /** Constructor. */
    public MultiStepPlan(
            PlanId id,
            SqlQueryType type,
            IgniteRel root,
            ResultSetMetadata meta,
            ParameterMetadata parameterMetadata,
            int catalogVersion,
            int numSources,
            @Nullable QueryPlan fastPlan,
            @Nullable PartitionAwarenessMetadata partitionAwarenessMetadata,
            @Nullable PartitionPruningMetadata partitionPruningMetadata
    ) {
        this.id = id;
        this.type = type;
        this.root = root;
        this.meta = meta;
        this.parameterMetadata = parameterMetadata;
        this.catalogVersion = catalogVersion;
        this.fastPlan = fastPlan;
        this.numSources = numSources;
        this.partitionAwarenessMetadata = partitionAwarenessMetadata;
        this.partitionPruningMetadata = partitionPruningMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public PlanId id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override
    public ParameterMetadata parameterMetadata() {
        return parameterMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionAwarenessMetadata partitionAwarenessMetadata() {
        return partitionAwarenessMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionPruningMetadata partitionPruningMetadata() {
        return partitionPruningMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public int numSources() {
        return numSources;
    }

    /** {@inheritDoc} */
    @Override
    public SqlQueryType type() {
        return type;
    }

    @Override
    public String explain() {
        IgniteRel clonedRoot = Cloner.clone(root, Commons.cluster());

        return ExplainUtils.toString(clonedRoot);
    }

    public int catalogVersion() {
        return catalogVersion;
    }

    /** Returns root of the query tree. */
    @Override
    public IgniteRel getRel() {
        return root;
    }

    /** Alternative fast plan. */
    public @Nullable QueryPlan fastPlan() {
        return fastPlan;
    }

    @Override
    public int tableId() {
        throw new UnsupportedOperationException("Should not be called");
    }
}
