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

import java.util.List;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlExplainMode;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Query explain plan.
 */
public class ExplainPlan implements QueryPlan {
    /** Explain metadata holder. */
    private static final ResultSetMetadata EXPLAIN_METADATA = new ResultSetMetadataImpl(List.of(
            new ColumnMetadataImpl("PLAN", ColumnType.STRING,
                    ColumnMetadata.UNDEFINED_PRECISION, ColumnMetadata.UNDEFINED_SCALE, true, null)));

    private final PlanId id;
    private final ExplainablePlan plan;
    private final IgniteSqlExplainMode mode;

    ExplainPlan(PlanId id, ExplainablePlan plan, IgniteSqlExplainMode mode) {
        this.id = id;
        this.plan = plan;
        this.mode = mode;
    }

    /** {@inheritDoc} */
    @Override
    public PlanId id() {
        return id;
    }

    public IgniteSqlExplainMode mode() {
        return mode;
    }

    /** {@inheritDoc} */
    @Override
    public SqlQueryType type() {
        return SqlQueryType.EXPLAIN;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return EXPLAIN_METADATA;
    }

    /** {@inheritDoc} */
    @Override
    public ParameterMetadata parameterMetadata() {
        return plan.parameterMetadata();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionAwarenessMetadata partitionAwarenessMetadata() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionPruningMetadata partitionPruningMetadata() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public int numSources() {
        return plan.numSources();
    }

    public ExplainablePlan plan() {
        return plan;
    }
}
