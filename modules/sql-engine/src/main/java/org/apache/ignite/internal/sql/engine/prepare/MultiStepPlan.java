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
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Regular query or DML.
 */
public class MultiStepPlan implements QueryPlan {

    private final SqlQueryType type;

    protected final ResultSetMetadata meta;

    protected final List<Fragment> fragments;

    /** Constructor. */
    public MultiStepPlan(SqlQueryType type, List<Fragment> fragments, ResultSetMetadata meta) {
        this.type = type;
        this.fragments = fragments;
        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public SqlQueryType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override
    public QueryPlan copy() {
        return new MultiStepPlan(type, fragments, meta);
    }

    /** A list for fragment this query plan consists of. */
    public List<Fragment> fragments() {
        return fragments;
    }
}
