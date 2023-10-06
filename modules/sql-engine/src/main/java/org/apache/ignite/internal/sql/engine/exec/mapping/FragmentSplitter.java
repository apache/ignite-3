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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.IdGenerator;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
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
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Splits the fragment onto two by given cut point.
 */
class FragmentSplitter extends IgniteRelShuttle {
    private final Deque<FragmentProto> stack = new LinkedList<>();

    private RelNode cutPoint;

    private FragmentProto curr;

    private boolean correlated = false;

    FragmentSplitter(RelNode cutPoint) {
        this.cutPoint = cutPoint;
    }

    /** Returns list of fragments, which were calculated by splitting original fragment by given cut point. */
    public List<Fragment> go(Fragment fragment) {
        ArrayList<Fragment> res = new ArrayList<>();

        stack.push(new FragmentProto(IdGenerator.nextId(), fragment.correlated(), fragment.root()));

        while (!stack.isEmpty()) {
            curr = stack.pop();

            correlated = curr.correlated;

            curr.root = visit(curr.root);
            res.add(curr.build());
            curr = null;
        }

        return res;
    }

    @Override
    public IgniteRel visit(IgniteSystemViewScan rel) {
        IgniteSystemView view = rel.getTable().unwrap(IgniteSystemView.class);

        assert view != null;

        if (curr.seenRelations.add(view.id())) {
            curr.systemViews.add(view);
        }

        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteTableScan rel) {
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

        assert table != null;

        if (curr.seenRelations.add(table.id())) {
            curr.tables.add(table);
        }

        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteIndexScan rel) {
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

        assert table != null;

        if (curr.seenRelations.add(table.id())) {
            curr.tables.add(table);
        }

        return super.visit(rel);
    }

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
    public IgniteRel visit(IgniteReceiver rel) {
        curr.remotes.add(rel);

        return rel;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteExchange rel) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteCorrelatedNestedLoopJoin rel) {
        if (rel == cutPoint) {
            cutPoint = null;

            return split(rel);
        }

        List<IgniteRel> inputs = Commons.cast(rel.getInputs());

        assert inputs.size() == 2;

        visitChild(rel, 0, inputs.get(0));

        boolean correlatedBefore = correlated;

        correlated = true;
        visitChild(rel, 1, inputs.get(1));
        correlated = correlatedBefore;

        return rel;
    }

    /**
     * Visits all children of a parent.
     */
    @Override
    protected IgniteRel processNode(IgniteRel rel) {
        if (rel == cutPoint) {
            cutPoint = null;

            return split(rel);
        }

        List<IgniteRel> inputs = Commons.cast(rel.getInputs());

        for (int i = 0; i < inputs.size(); i++) {
            visitChild(rel, i, inputs.get(i));
        }

        return rel;
    }

    private IgniteRel split(IgniteRel rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = rel.getTraitSet();
        RelDataType rowType = rel.getRowType();

        RelNode input = rel instanceof IgniteTrimExchange ? rel.getInput(0) : rel;

        long targetFragmentId = curr.id;
        long sourceFragmentId = IdGenerator.nextId();
        long exchangeId = sourceFragmentId;

        IgniteReceiver receiver = new IgniteReceiver(cluster, traits, rowType, exchangeId, sourceFragmentId);
        IgniteSender sender = new IgniteSender(cluster, traits, input, exchangeId, targetFragmentId, rel.distribution());

        curr.remotes.add(receiver);
        stack.push(new FragmentProto(sourceFragmentId, correlated, sender));

        return receiver;
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
