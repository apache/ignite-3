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
import org.apache.ignite.internal.sql.api.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.api.ResultSetMetadataImpl;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Query explain plan.
 */
public class  ExplainPlan implements QueryPlan {
    /** Explain metadata holder. */
    private static final ResultSetMetadata EXPLAIN_METADATA = new ResultSetMetadataImpl(List.of(
            new ColumnMetadataImpl("PLAN", ColumnType.STRING, -1, Integer.MIN_VALUE, true, null)));

    private final String plan;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ExplainPlan(String plan) {
        this.plan = plan;
    }

    /** {@inheritDoc} */
    @Override public Type type() {
        return Type.EXPLAIN;
    }

    /** {@inheritDoc} */
    @Override public QueryPlan copy() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public ResultSetMetadata metadata() {
        return EXPLAIN_METADATA;
    }

    public String plan() {
        return plan;
    }
}
