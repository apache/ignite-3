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
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.SourceAwareIgniteRel;

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
        CloneAndAssignIds assigner = new CloneAndAssignIds(cluster);
        IgniteRel result = assigner.visit(root);
        int numSources = assigner.sourceIndex;

        return new IntObjectImmutablePair<>(numSources, result);
    }

    private static class CloneAndAssignIds extends IgniteRelShuttle {

        private final RelOptCluster cluster;

        private int sourceIndex;

        private CloneAndAssignIds(RelOptCluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public IgniteRel visit(IgniteRel rel) {
            if (rel instanceof SourceAwareIgniteRel) {
                // Take into account only actual source relations.
                SourceAwareIgniteRel src = (SourceAwareIgniteRel) rel;

                int sourceId = sourceIndex++;
                IgniteRel relWithSourceId = src.clone(sourceId);

                for (int i = 0; i < rel.getInputs().size(); i++) {
                    IgniteRel childNode = visit((IgniteRel) rel.getInput(i));
                    relWithSourceId.replaceInput(i, childNode);
                }

                return relWithSourceId;
            } else {
                List<IgniteRel> newChildren = new ArrayList<>(rel.getInputs().size());

                for (int i = 0; i < rel.getInputs().size(); i++) {
                    IgniteRel childNode = visit((IgniteRel) rel.getInput(i));
                    newChildren.add(childNode);
                }

                return rel.clone(cluster, newChildren);
            }
        }
    }
}
