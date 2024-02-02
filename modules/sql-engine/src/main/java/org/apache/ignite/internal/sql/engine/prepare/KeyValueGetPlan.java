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

import java.util.concurrent.ExecutorService;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.rel.IgnitePkLookup;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Plan representing single lookup by a primary key.
 *
 * <p>Note: since {@link IgnitePkLookup} is not supposed to be a part of distributed
 * query plan, we need a new object to be able to handle it differently by {@link ExecutorService}.
 */
public class KeyValueGetPlan implements ExplainablePlan {
    private final PlanId id;
    private final IgnitePkLookup lookupNode;
    private final ResultSetMetadata meta;
    private final ParameterMetadata parameterMetadata;

    KeyValueGetPlan(PlanId id, IgnitePkLookup lookupNode, ResultSetMetadata meta, ParameterMetadata parameterMetadata) {
        this.id = id;
        this.lookupNode = lookupNode;
        this.meta = meta;
        this.parameterMetadata = parameterMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public PlanId id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public SqlQueryType type() {
        return SqlQueryType.QUERY;
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

    /** Returns a table in question. */
    public IgniteTable table() {
        IgniteTable table = lookupNode.getTable().unwrap(IgniteTable.class);

        assert table != null : lookupNode.getTable();

        return table;
    }

    @Override
    public String explain() {
        IgniteRel clonedRoot = Cloner.clone(lookupNode, Commons.cluster());

        return RelOptUtil.toString(clonedRoot, SqlExplainLevel.ALL_ATTRIBUTES);
    }

    public IgnitePkLookup lookupNode() {
        return lookupNode;
    }
}
