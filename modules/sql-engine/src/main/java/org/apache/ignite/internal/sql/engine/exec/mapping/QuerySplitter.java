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
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.rel.SourceAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Splits a query into a list of query fragments.
 *
 * <p>Distributed query tree may have number of {@link IgniteExchange} operators. These operators
 * represents points where one distribution should be converted to another. In runtime, this operation
 * implemented as physical exchange of messages between two nodes (or itself, it's intentional tradeoff).
 *
 * <p>{@link QuerySplitter} is meant to find all {@link IgniteExchange} operators, cut the tree in that places,
 * and replace single exchange with {@link IgniteReceiver} and {@link IgniteSender} on target and source fragments
 * accordingly. Here is an example for a simple `SELECT * FROM test` query:
 *
 * <pre>
 *     Here is a relation tree representing the query:
 *
 *      IgniteExchange(distribution=[single]) ------------| we've got exchange here because table distributed by affinity, meaning
 *          IgniteTableScan(table=[[PUBLIC, TEST]])       | every node owns only a few partitions of that table. Thus, in order to
 *                                                        | get all rows in a single place, we need to resend them. In general it
 *                                                        | means, that messages will be sent over network, but in some cases it
 *     After splitting it will be represented             | may be local call as well (for instance, it is a single node cluster)
 *     by two fragments:
 *
 *     Fragment#1:
 *          IgniteReceiver(source=Fragment#2, exchangeId=3)
 *
 *     Fragment#2:
 *          IgniteSender(target=Fragment#1, exchangeId=3)
 *              IgniteTableScan(table=[[PUBLIC, TEST]])
 * </pre>
 */
public class QuerySplitter extends IgniteRelShuttle {
    private final Deque<FragmentProto> stack = new LinkedList<>();

    private final RelOptCluster cluster;
    private final IdGenerator idGenerator;

    private FragmentProto curr;

    private boolean correlated = false;

    public QuerySplitter(IdGenerator idGenerator, RelOptCluster cluster) {
        this.idGenerator = idGenerator;
        this.cluster = cluster;
    }

    /**
     * Splits given relation tree on a list of {@link Fragment fragments}.
     *
     * <p>See class-level javadoc for details.
     */
    public List<Fragment> split(IgniteRel root) {
        ArrayList<Fragment> res = new ArrayList<>();

        stack.push(new FragmentProto(idGenerator.nextId(), false, root));

        while (!stack.isEmpty()) {
            curr = stack.pop();

            // We need to clone it after CALCITE-5503, otherwise it become possible to obtain equals multiple inputs i.e.:
            //          rel#348IgniteExchange
            //          rel#287IgniteMergeJoin
            //       _____|             |_____
            //       V                       V
            //   IgniteSort#285            IgniteSort#285
            //   IgniteTableScan#180       IgniteTableScan#180
            curr.root = Cloner.clone(curr.root, cluster);

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
        long sourceFragmentId = idGenerator.nextId();
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
        return ((SourceAwareIgniteRel) processNode(rel)).clone(idGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteIndexScan rel) {
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

        assert table != null;
        long sourceId = idGenerator.nextId();

        curr.tables.put(sourceId, table);

        return rel.clone(sourceId);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteTableScan rel) {
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

        assert table != null;
        long sourceId = idGenerator.nextId();

        curr.tables.put(sourceId, table);

        return rel.clone(sourceId);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteTableFunctionScan rel) {
        return rel.clone(idGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel visit(IgniteTableModify rel) {
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

        assert table != null;

        long sourceId = idGenerator.nextId();

        curr.tables.put(sourceId, table);

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

        if (curr.seenRelations.add(view.id())) {
            curr.systemViews.add(view);
        }

        return rel.clone(idGenerator.nextId());
    }

    private static class FragmentProto {
        private final long id;
        private final boolean correlated;

        private IgniteRel root;

        private final IntSet seenRelations = new IntOpenHashSet();

        private final List<IgniteReceiver> remotes = new ArrayList<>();
        private final Long2ObjectMap<IgniteTable> tables = new Long2ObjectOpenHashMap<>();
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
