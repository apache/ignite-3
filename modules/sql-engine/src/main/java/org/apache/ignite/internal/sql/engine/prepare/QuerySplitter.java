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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.rel.SourceAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Splits a query into a list of query fragments.
 */
public class QuerySplitter extends IgniteRelShuttle {
    private final Deque<FragmentProto> stack = new LinkedList<>();

    private FragmentProto curr;

    private boolean correlated = false;

    /**
     * Go.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public List<Fragment> go(IgniteRel root) {
        ArrayList<Fragment> res = new ArrayList<>();

        stack.push(new FragmentProto(IdGenerator.nextId(), false, root));

        while (!stack.isEmpty()) {
            curr = stack.pop();

            // We need to clone it after CALCITE-5503, otherwise it become possible to obtain equals multiple inputs i.e.:
            //          rel#348IgniteExchange
            //          rel#287IgniteMergeJoin
            //       _____|             |_____
            //       V                       V
            //   IgniteSort#285            IgniteSort#285
            //   IgniteTableScan#180       IgniteTableScan#180
            curr.root = Cloner.clone(curr.root);

            correlated = curr.correlated;

            curr.root = visit(curr.root);

            res.add(curr.build());
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
    public IgniteRel visit(IgniteCorrelatedNestedLoopJoin rel) {
        List<IgniteRel> inputs = Commons.cast(rel.getInputs());

        assert inputs.size() == 2;

        visitChild(rel, 0, inputs.get(0));

        boolean correlatedBefore = correlated;

        correlated = true;
        visitChild(rel, 1, inputs.get(1));
        correlated = correlatedBefore;

        return rel;
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
        stack.push(new FragmentProto(sourceFragmentId, correlated, sender));

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
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

        assert table != null;

        if (curr.seenRelations.add(table.id())) {
            curr.tables.add(table);
        }

        return rel.clone(IdGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteTableScan rel) {
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

        assert table != null;

        if (curr.seenRelations.add(table.id())) {
            curr.tables.add(table);
        }

        return rel.clone(IdGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteTableModify rel) {
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

        assert table != null;

        if (curr.seenRelations.add(table.id())) {
            curr.tables.add(table);
        }

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteSystemViewScan rel) {
        IgniteSystemView view = rel.getTable().unwrap(IgniteSystemView.class);

        assert view != null;

        if (curr.seenRelations.add(view.id())) {
            curr.systemViews.add(view);
        }

        return rel.clone(IdGenerator.nextId());
    }

    private static class FragmentProto {
        private final long id;
        private final boolean correlated;

        private IgniteRel root;

        private final IntSet seenRelations = new IntOpenHashSet();

        private final List<IgniteReceiver> remotes = new ArrayList<>();
        private final List<IgniteTable> tables = new ArrayList<>();
        private final List<IgniteSystemView> systemViews = new ArrayList<>();

        private FragmentProto(long id, boolean correlated, IgniteRel root) {
            this.id = id;
            this.correlated = correlated;
            this.root = root;
        }

        Fragment build() {
            return new Fragment(id, correlated, root, remotes, tables, systemViews);
        }
    }
}
