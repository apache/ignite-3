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
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;

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

    ExplainPlan(PlanId id, ExplainablePlan plan) {
        this.id = id;
        this.plan = plan;
    }

    /** {@inheritDoc} */
    @Override
    public PlanId id() {
        return id;
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

    public ExplainablePlan plan() {
        return plan;
    }
}
