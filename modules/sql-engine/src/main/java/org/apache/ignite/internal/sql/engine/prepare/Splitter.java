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

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.rel.SourceAwareIgniteRel;

/**
 * Splits a query into a list of query fragments.
 */
public class Splitter extends IgniteRelShuttle {
    private final Deque<FragmentProto> stack = new LinkedList<>();

    private FragmentProto curr;

    /**
     * Go.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public List<Fragment> go(IgniteRel root) {
        ArrayList<Fragment> res = new ArrayList<>();

        stack.push(new FragmentProto(IdGenerator.nextId(), root));

        while (!stack.isEmpty()) {
            curr = stack.pop();

            curr.root = visit(curr.root);

            res.add(curr.build());

            curr = null;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteReceiver rel) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteExchange rel) {
        RelOptCluster cluster = rel.getCluster();

        long targetFragmentId = curr.id;
        long sourceFragmentId = IdGenerator.nextId();
        long exchangeId = sourceFragmentId;

        IgniteReceiver receiver = new IgniteReceiver(cluster, rel.getTraitSet(), rel.getRowType(), exchangeId, sourceFragmentId);
        IgniteSender sender = new IgniteSender(cluster, rel.getTraitSet(), rel.getInput(), exchangeId, targetFragmentId,
                rel.distribution());

        curr.remotes.add(receiver);
        stack.push(new FragmentProto(sourceFragmentId, sender));

        return receiver;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteTrimExchange rel) {
        return ((SourceAwareIgniteRel) processNode(rel)).clone(IdGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteIndexScan rel) {
        return rel.clone(IdGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteTableScan rel) {
        return rel.clone(IdGenerator.nextId());
    }

    private static class FragmentProto {
        private final long id;

        private IgniteRel root;

        private final List<IgniteReceiver> remotes = new ArrayList<>();

        private FragmentProto(long id, IgniteRel root) {
            this.id = id;
            this.root = root;
        }

        Fragment build() {
            return new Fragment(id, root, List.copyOf(remotes));
        }
    }
}
