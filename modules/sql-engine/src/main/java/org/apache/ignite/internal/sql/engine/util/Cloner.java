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

package org.apache.ignite.internal.sql.engine.util;

import it.unimi.dsi.fastutil.ints.IntObjectImmutablePair;
import it.unimi.dsi.fastutil.ints.IntObjectPair;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;

/**
 * Utility class to clone relational tree and optionally replace assigned {@link RelOptCluster cluster}
 * to another one.
 */
public class Cloner {
    private final RelOptCluster cluster;

    private Cloner(RelOptCluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Clones a given relational tree and reassigns copy to a given cluster.
     *
     * @param root Root of the tree to clone.
     * @param cluster Cluster to attach copy to.
     * @return Copy of the given tree.
     */
    public static IgniteRel clone(IgniteRel root, RelOptCluster cluster) {
        Cloner c = new Cloner(cluster);

        return c.visit(root);
    }

    private IgniteRel visit(IgniteRel rel) {
        return rel.clone(cluster, Commons.transform(rel.getInputs(), rel0 -> visit((IgniteRel) rel0)));
    }


    /**
     * Clones and assigns source ids to all source relations.
     * 
     * @param root Plan.
     * @param cluster Cluster.
     * @return The number of source relations in the given plan and the plan itself.
     */
    public static IntObjectPair<IgniteRel> cloneAndAssignSourceId(IgniteRel root, RelOptCluster cluster) {
        Cloner c = new Cloner(cluster);

        IgniteRel cloned = c.visit(root);
        AssignSourceId ids = new AssignSourceId();
        // Assign ids
        IgniteRel result = cloned.accept(ids);
        // Get ids
        int numSources = ids.sourceIndex;

        return new IntObjectImmutablePair<>(numSources, result);
    }

    private static class AssignSourceId extends IgniteRelShuttle {

        private int sourceIndex;

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteIndexScan rel) {
            IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

            assert table != null;
            long sourceId = sourceIndex++;

            return rel.clone(sourceId);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteTableScan rel) {
            IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

            assert table != null;
            long sourceId = sourceIndex++;

            return rel.clone(sourceId);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteTableFunctionScan rel) {
            return rel.clone(sourceIndex++);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteTableModify rel) {
            IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

            assert table != null;

            long sourceId = sourceIndex++;

            IgniteRel cloned = rel.clone(sourceId);
            IgniteRel input = this.visit((IgniteRel) rel.getInput(0));

            cloned.replaceInput(0, input);

            return cloned;
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteSystemViewScan rel) {
            IgniteSystemView view = rel.getTable().unwrap(IgniteSystemView.class);

            assert view != null;

            return rel.clone(sourceIndex++);
        }
    }
}
