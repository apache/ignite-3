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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.combinedRowType;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.CollectionUtils.first;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractSetOpNode;
import org.apache.ignite.internal.sql.engine.exec.rel.CorrelatedNestedLoopJoinNode;
import org.apache.ignite.internal.sql.engine.exec.rel.FilterNode;
import org.apache.ignite.internal.sql.engine.exec.rel.HashAggregateNode;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.IndexScanNode;
import org.apache.ignite.internal.sql.engine.exec.rel.IndexSpoolNode;
import org.apache.ignite.internal.sql.engine.exec.rel.IntersectNode;
import org.apache.ignite.internal.sql.engine.exec.rel.LimitNode;
import org.apache.ignite.internal.sql.engine.exec.rel.MergeJoinNode;
import org.apache.ignite.internal.sql.engine.exec.rel.MinusNode;
import org.apache.ignite.internal.sql.engine.exec.rel.ModifyNode;
import org.apache.ignite.internal.sql.engine.exec.rel.NestedLoopJoinNode;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.exec.rel.ProjectNode;
import org.apache.ignite.internal.sql.engine.exec.rel.ScanNode;
import org.apache.ignite.internal.sql.engine.exec.rel.SortAggregateNode;
import org.apache.ignite.internal.sql.engine.exec.rel.SortNode;
import org.apache.ignite.internal.sql.engine.exec.rel.TableScanNode;
import org.apache.ignite.internal.sql.engine.exec.rel.TableSpoolNode;
import org.apache.ignite.internal.sql.engine.exec.rel.UnionAllNode;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteSetOp;
import org.apache.ignite.internal.sql.engine.rule.LogicalScanConverterRule;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchemaIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.Destination;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactory;

/**
 * Implements a query plan.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class LogicalRelImplementor<RowT> implements IgniteRelVisitor<Node<RowT>> {
    public static final String CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG =
            "only INNER and LEFT join supported by IgniteCorrelatedNestedLoop";

    private final ExecutionContext<RowT> ctx;

    private final HashFunctionFactory<RowT> hashFuncFactory;

    private final ExchangeService exchangeSvc;

    private final MailboxRegistry mailboxRegistry;

    private final ExpressionFactory<RowT> expressionFactory;

    private final ResolvedDependencies resolvedDependencies;

    /**
     * Constructor.
     *
     * @param ctx Root context.
     * @param hashFuncFactory Factory to create a hash function for the row, from which the destination nodes are calculated.
     * @param mailboxRegistry Mailbox registry.
     * @param exchangeSvc Exchange service.
     * @param resolvedDependencies  Dependencies required to execute this query.
     */
    public LogicalRelImplementor(
            ExecutionContext<RowT> ctx,
            HashFunctionFactory<RowT> hashFuncFactory,
            MailboxRegistry mailboxRegistry,
            ExchangeService exchangeSvc,
            ResolvedDependencies resolvedDependencies) {
        this.hashFuncFactory = hashFuncFactory;
        this.mailboxRegistry = mailboxRegistry;
        this.exchangeSvc = exchangeSvc;
        this.ctx = ctx;
        this.resolvedDependencies = resolvedDependencies;

        expressionFactory = ctx.expressionFactory();
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSender rel) {
        IgniteDistribution distribution = rel.distribution();

        Destination<RowT> dest = distribution.destination(hashFuncFactory, ctx.target());

        // Outbox fragment ID is used as exchange ID as well.
        Outbox<RowT> outbox = new Outbox<>(ctx, exchangeSvc, mailboxRegistry, rel.exchangeId(), rel.targetFragmentId(), dest);

        Node<RowT> input = visit(rel.getInput());

        outbox.register(input);

        mailboxRegistry.register(outbox);

        return outbox;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteFilter rel) {
        Predicate<RowT> pred = expressionFactory.predicate(rel.getCondition(), rel.getRowType());

        FilterNode<RowT> node = new FilterNode<>(ctx, pred);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTrimExchange rel) {
        assert TraitUtils.distribution(rel).getType() == HASH_DISTRIBUTED;

        IgniteDistribution distr = rel.distribution();
        Destination<RowT> dest = distr.destination(hashFuncFactory, ctx.group(rel.sourceId()));
        String localNodeName = ctx.localNode().name();

        FilterNode<RowT> node = new FilterNode<>(ctx, r -> Objects.equals(localNodeName, first(dest.targets(r))));

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteProject rel) {
        Function<RowT, RowT> prj = expressionFactory.project(rel.getProjects(), rel.getInput().getRowType());

        ProjectNode<RowT> node = new ProjectNode<>(ctx, prj);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteNestedLoopJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        JoinRelType joinType = rel.getJoinType();

        RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);
        BiPredicate<RowT, RowT> cond = expressionFactory.biPredicate(rel.getCondition(), rowType);

        Node<RowT> node = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, joinType, cond);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteCorrelatedNestedLoopJoin rel) {
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();

        RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);
        BiPredicate<RowT, RowT> cond = expressionFactory.biPredicate(rel.getCondition(), rowType);

        assert rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.LEFT
                : CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG;

        RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightType);

        Node<RowT> node = new CorrelatedNestedLoopJoinNode<>(ctx, cond, rel.getVariablesSet(),
                rel.getJoinType(), rightRowFactory);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteMergeJoin rel) {
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        JoinRelType joinType = rel.getJoinType();

        int pairsCnt = rel.analyzeCondition().pairs().size();

        ImmutableBitSet leftKeys = rel.analyzeCondition().leftSet();

        List<RexNode> conjunctions = RelOptUtil.conjunctions(rel.getCondition());

        ImmutableBitSet.Builder nullCompAsEqualBuilder = ImmutableBitSet.builder();

        ImmutableBitSet nullCompAsEqual;
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int idx = ref.getIndex();
                if (leftKeys.get(idx)) {
                    nullCompAsEqualBuilder.set(idx);
                }
                return ref;
            }
        };

        for (RexNode expr : conjunctions) {
            if (expr.getKind() == SqlKind.IS_NOT_DISTINCT_FROM) {
                shuttle.apply(expr);
            }
        }

        nullCompAsEqual = nullCompAsEqualBuilder.build();

        Comparator<RowT> comp = expressionFactory.comparator(
                rel.leftCollation().getFieldCollations().subList(0, pairsCnt),
                rel.rightCollation().getFieldCollations().subList(0, pairsCnt),
                nullCompAsEqual
        );

        Node<RowT> node = MergeJoinNode.create(ctx, leftType, rightType, joinType, comp);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteIndexScan rel) {
        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();
        ImmutableBitSet requiredColumns = rel.requiredColumns();
        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);
        ScannableTable scannableTable = resolvedDependencies.scannableTable(tbl.id());

        IgniteSchemaIndex idx = tbl.indexes().get(rel.indexName());

        List<SearchBounds> searchBounds = rel.searchBounds();
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();

        Predicate<RowT> filters = condition == null ? null : expressionFactory.predicate(condition, rowType);
        Function<RowT, RowT> prj = projects == null ? null : expressionFactory.project(projects, rowType);

        RangeIterable<RowT> ranges = null;

        if (searchBounds != null) {
            Comparator<RowT> searchRowComparator = null;

            if (idx.type() != Type.SORTED) {
                searchRowComparator = expressionFactory.comparator(idx.collation());
            }

            ranges = expressionFactory.ranges(searchBounds, idx.getRowType(typeFactory, tbl.descriptor()), searchRowComparator);
        }

        RelCollation outputCollation = rel.collation();

        if (projects != null || requiredColumns != null) {
            outputCollation = outputCollation.apply(LogicalScanConverterRule.createMapping(
                    projects,
                    requiredColumns,
                    tbl.getRowType(typeFactory).getFieldCount()
            ));
        }

        ColocationGroup group = ctx.group(rel.sourceId());
        Comparator<RowT> comp = idx.type() == Type.SORTED ? ctx.expressionFactory().comparator(outputCollation) : null;

        if (!group.nodeNames().contains(ctx.localNode().name())) {
            return new ScanNode<>(ctx, Collections.emptyList());
        }

        return new IndexScanNode<>(
                ctx,
                ctx.rowHandler().factory(ctx.getTypeFactory(), rowType),
                idx,
                scannableTable,
                tbl.descriptor(),
                group.partitionsWithTerms(ctx.localNode().name()),
                comp,
                ranges,
                filters,
                prj,
                requiredColumns == null ? null : requiredColumns.toBitSet()
        );
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableScan rel) {
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();
        ImmutableBitSet requiredColumns = rel.requiredColumns();

        IgniteTable tbl = rel.getTable().unwrapOrThrow(IgniteTable.class);
        ScannableTable scannableTable = resolvedDependencies.scannableTable(tbl.id());

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);

        Predicate<RowT> filters = condition == null ? null : expressionFactory.predicate(condition, rowType);
        Function<RowT, RowT> prj = projects == null ? null : expressionFactory.project(projects, rowType);

        ColocationGroup group = ctx.group(rel.sourceId());

        if (!group.nodeNames().contains(ctx.localNode().name())) {
            return new ScanNode<>(ctx, Collections.emptyList());
        }

        return new TableScanNode<>(
                ctx,
                ctx.rowHandler().factory(ctx.getTypeFactory(), rowType),
                scannableTable,
                group.partitionsWithTerms(ctx.localNode().name()),
                filters,
                prj,
                requiredColumns == null ? null : requiredColumns.toBitSet()
        );
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteValues rel) {
        List<RexLiteral> vals = Commons.flat(Commons.cast(rel.getTuples()));

        RelDataType rowType = rel.getRowType();

        return new ScanNode<>(ctx, expressionFactory.values(vals, rowType));
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteUnionAll rel) {
        UnionAllNode<RowT> node = new UnionAllNode<>(ctx);

        List<Node<RowT>> inputs = Commons.transform(rel.getInputs(), this::visit);

        node.register(inputs);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteLimit rel) {
        Supplier<Integer> offset = (rel.offset() == null) ? null : expressionFactory.execute(rel.offset());
        Supplier<Integer> fetch = (rel.fetch() == null) ? null : expressionFactory.execute(rel.fetch());

        LimitNode<RowT> node = new LimitNode<>(ctx, offset, fetch);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSort rel) {
        RelCollation collation = rel.getCollation();

        Supplier<Integer> offset = (rel.offset == null) ? null : expressionFactory.execute(rel.offset);
        Supplier<Integer> fetch = (rel.fetch == null) ? null : expressionFactory.execute(rel.fetch);

        SortNode<RowT> node = new SortNode<>(ctx, expressionFactory.comparator(collation), offset,
                fetch);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableSpool rel) {
        TableSpoolNode<RowT> node = new TableSpoolNode<>(ctx, rel.readType == Spool.Type.LAZY);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSortedIndexSpool rel) {
        RelCollation collation = rel.collation();

        assert rel.searchBounds() != null : rel;

        Predicate<RowT> filter = expressionFactory.predicate(rel.condition(), rel.getRowType());
        Comparator<RowT> comparator = expressionFactory.comparator(collation);
        RangeIterable<RowT> ranges = expressionFactory.ranges(rel.searchBounds(), rel.getRowType(), comparator);

        IndexSpoolNode<RowT> node = IndexSpoolNode.createTreeSpool(
                ctx,
                rel.getRowType(),
                collation,
                comparator,
                filter,
                ranges
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteHashIndexSpool rel) {
        Supplier<RowT> searchRow = expressionFactory.rowSource(rel.searchRow());

        Predicate<RowT> filter = expressionFactory.predicate(rel.condition(), rel.getRowType());

        IndexSpoolNode<RowT> node = IndexSpoolNode.createHashSpool(
                ctx,
                ImmutableBitSet.of(rel.keys()),
                filter,
                searchRow,
                rel.allowNulls()
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSetOp rel) {
        RelDataType rowType = rel.getRowType();

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        List<Node<RowT>> inputs = Commons.transform(rel.getInputs(), this::visit);

        AbstractSetOpNode<RowT> node;

        if (rel instanceof Minus) {
            node = new MinusNode<>(ctx, rel.aggregateType(), rel.all(), rowFactory);
        } else if (rel instanceof Intersect) {
            node = new IntersectNode<>(ctx, rel.aggregateType(), rel.all(), rowFactory, rel.getInputs().size());
        } else {
            throw new AssertionError();
        }

        node.register(inputs);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableFunctionScan rel) {
        Supplier<Iterable<Object[]>> dataSupplier = expressionFactory.execute(rel.getCall());

        RelDataType rowType = rel.getRowType();

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        return new ScanNode<>(ctx, new TableFunctionScan<>(dataSupplier, rowFactory));
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableModify rel) {
        IgniteTable table = rel.getTable().unwrapOrThrow(IgniteTable.class);
        UpdatableTable updatableTable = resolvedDependencies.updatableTable(table.id());

        ModifyNode<RowT> node = new ModifyNode<>(ctx, updatableTable, rel.getOperation(), rel.getUpdateColumnList());

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReceiver rel) {
        Inbox<RowT> inbox = new Inbox<>(ctx, exchangeSvc, mailboxRegistry,
                ctx.remotes(rel.exchangeId()), expressionFactory.comparator(rel.collation()),
                rel.exchangeId(), rel.sourceFragmentId());

        mailboxRegistry.register(inbox);

        return inbox;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteColocatedHashAggregate rel) {
        AggregateType type = AggregateType.SINGLE;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type, rel.getAggCallList(), inputType);
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteMapHashAggregate rel) {
        AggregateType type = AggregateType.MAP;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type, rel.getAggCallList(), inputType);
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReduceHashAggregate rel) {
        AggregateType type = AggregateType.REDUCE;

        RelDataType rowType = rel.getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type, rel.getAggregateCalls(), null);
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteColocatedSortAggregate rel) {
        AggregateType type = AggregateType.SINGLE;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type,
                rel.getAggCallList(),
                inputType
        );

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        Comparator<RowT> comp = expressionFactory.comparator(rel.collation());

        if (rel.getGroupSet().isEmpty() && comp == null) {
            comp = (k1, k2) -> 0;
        }

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                type,
                rel.getGroupSet(),
                accFactory,
                rowFactory,
                comp
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteMapSortAggregate rel) {
        AggregateType type = AggregateType.MAP;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type,
                rel.getAggCallList(),
                inputType
        );

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        Comparator<RowT> comp = expressionFactory.comparator(rel.collation());

        if (rel.getGroupSet().isEmpty() && comp == null) {
            comp = (k1, k2) -> 0;
        }

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                type,
                rel.getGroupSet(),
                accFactory,
                rowFactory,
                comp
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReduceSortAggregate rel) {
        AggregateType type = AggregateType.REDUCE;

        RelDataType rowType = rel.getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type,
                rel.getAggregateCalls(),
                null
        );

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        Comparator<RowT> comp = expressionFactory.comparator(rel.collation());

        if (rel.getGroupSet().isEmpty() && comp == null) {
            comp = (k1, k2) -> 0;
        }

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                type,
                rel.getGroupSet(),
                accFactory,
                rowFactory,
                comp
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteExchange rel) {
        throw new AssertionError();
    }

    private Node<RowT> visit(RelNode rel) {
        return visit((IgniteRel) rel);
    }

    @SuppressWarnings("unchecked")
    public <T extends Node<RowT>> T go(IgniteRel rel) {
        return (T) visit(rel);
    }
}
