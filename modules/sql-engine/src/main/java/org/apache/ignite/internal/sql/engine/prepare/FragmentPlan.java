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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.sql.engine.ResultSetMetadata;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * FragmentPlan.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class FragmentPlan implements QueryPlan {
    private final IgniteRel root;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public FragmentPlan(IgniteRel root) {
        RelOptCluster cluster = Commons.cluster();

        this.root = new Cloner(cluster).visit(root);
    }

    public IgniteRel root() {
        return root;
    }

    /** {@inheritDoc} */
    @Override
    public Type type() {
        return Type.FRAGMENT;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return List::of;
    }

    /** {@inheritDoc} */
    @Override
    public QueryPlan copy() {
        return this;
    }
}
