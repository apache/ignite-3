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
import static org.apache.ignite.internal.sql.engine.rule.LogicalScanConverterRule.createMapping;
import static org.apache.ignite.internal.sql.engine.util.Commons.cast;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.combinedRowType;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.convertStructuredType;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlComparator;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlExpressionFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlJoinPredicate;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlJoinProjection;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlPredicate;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlProjection;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlRowProvider;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlScalar;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunction;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistry;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractSetOpNode;
import org.apache.ignite.internal.sql.engine.exec.rel.CorrelatedNestedLoopJoinNode;
import org.apache.ignite.internal.sql.engine.exec.rel.DataSourceScanNode;
import org.apache.ignite.internal.sql.engine.exec.rel.FilterNode;
import org.apache.ignite.internal.sql.engine.exec.rel.HashAggregateNode;
import org.apache.ignite.internal.sql.engine.exec.rel.HashJoinNode;
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
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;
import org.apache.ignite.internal.sql.engine.rel.IgniteSelectCount;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
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
import org.apache.ignite.internal.sql.engine.rel.set.IgniteIntersect;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteMapSetOp;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteReduceIntersect;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteSetOp;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.Destination;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteMath;
import org.apache.ignite.internal.sql.engine.util.IgniteResource;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Implements a query plan.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class LogicalRelImplementor<RowT> implements IgniteRelVisitor<Node<RowT>> {
    private static final EnumSet<JoinRelType> JOIN_NEEDS_PROJECTION = EnumSet.of(
            JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.FULL, JoinRelType.RIGHT
    );

    private static final Comparator<IntPair> CONDITION_PAIRS_COMPARATOR = Comparator.comparingInt(
                    (IntPair l) -> Math.max(l.source, l.target)).thenComparingInt(l -> Math.min(l.source, l.target));

    public static final String CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG =
            "only INNER and LEFT join supported by IgniteCorrelatedNestedLoop";

    private final ExecutionContext<RowT> ctx;

    private final DestinationFactory<RowT> destinationFactory;

    private final ExchangeService exchangeSvc;

    private final MailboxRegistry mailboxRegistry;

    private final SqlExpressionFactory expressionFactory;

    private final ResolvedDependencies resolvedDependencies;

    private final TableFunctionRegistry tableFunctionRegistry;

    private @Nullable List<RexNode> projectionToFuse;

    /**
     * Constructor.
     *
     * @param ctx Root context.
     * @param mailboxRegistry Mailbox registry.
     * @param exchangeSvc Exchange service.
     * @param resolvedDependencies Dependencies required to execute this query.
     * @param tableFunctionRegistry Table function registry.
     */
    public LogicalRelImplementor(
            ExecutionContext<RowT> ctx,
            MailboxRegistry mailboxRegistry,
            ExchangeService exchangeSvc,
            ResolvedDependencies resolvedDependencies,
            TableFunctionRegistry tableFunctionRegistry
    ) {
        this.mailboxRegistry = mailboxRegistry;
        this.exchangeSvc = exchangeSvc;
        this.ctx = ctx;
        this.resolvedDependencies = resolvedDependencies;
        this.tableFunctionRegistry = tableFunctionRegistry;

        expressionFactory = ctx.expressionFactory();
        destinationFactory = new DestinationFactory<>(ctx.rowAccessor(), resolvedDependencies);
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSender rel) {
        IgniteDistribution distribution = rel.distribution();

        ColocationGroup targetGroup = ctx.target();

        assert targetGroup != null;

        Destination<RowT> dest = destinationFactory.createDestination(distribution, targetGroup);

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
        SqlPredicate sqlPredicate = expressionFactory.predicate(rel.getCondition(), rel.getRowType());
        Predicate<RowT> pred = row -> sqlPredicate.test(ctx, row);

        FilterNode<RowT> node = new FilterNode<>(ctx, pred);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTrimExchange rel) {
        assert TraitUtils.distribution(rel).getType() == HASH_DISTRIBUTED;

        ColocationGroup targetGroup = ctx.group(rel.sourceId());

        assert targetGroup != null;

        Destination<RowT> dest = destinationFactory.createDestination(rel.distribution(), targetGroup);

        String localNodeName = ctx.localNode().name();

        FilterNode<RowT> node = new FilterNode<>(ctx, r -> Objects.equals(localNodeName, first(dest.targets(r))));

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteProject rel) {
        if (projectionToFuse == null && canFuseProjectionInto(rel.getInput())) {
            projectionToFuse = rel.getProjects();

            return visit(rel.getInput());
        }

        SqlProjection sqlProjection = expressionFactory.project(rel.getProjects(), rel.getInput().getRowType());
        Function<RowT, RowT> prj = row -> sqlProjection.project(ctx, row);

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

        SqlJoinProjection joinProjection = createJoinProjection(rel, outType, leftType.getFieldCount());

        RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);
        SqlJoinPredicate joinPredicate = expressionFactory.joinPredicate(rel.getCondition(), rowType, leftType.getFieldCount());
        BiPredicate<RowT, RowT> cond = (left, right) -> joinPredicate.test(ctx, left, right);

        Node<RowT> node = NestedLoopJoinNode.create(ctx, joinProjection, leftType, rightType, joinType, cond);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteHashJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        JoinRelType joinType = rel.getJoinType();

        SqlJoinProjection joinProjection = createJoinProjection(rel, outType, leftType.getFieldCount());

        RexNode nonEquiConditionExpression = RexUtil.composeConjunction(
                Commons.rexBuilder(), rel.analyzeCondition().nonEquiConditions, true
        );

        BiPredicate<RowT, RowT> nonEquiCondition = null;
        if (nonEquiConditionExpression != null) {
            RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);

            SqlJoinPredicate nonEquiPredicate = expressionFactory.joinPredicate(
                    rel.getCondition(), rowType, leftType.getFieldCount()
            );
            nonEquiCondition = (left, right) -> nonEquiPredicate.test(ctx, left, right);
        }

        Node<RowT> node = HashJoinNode.create(ctx, joinProjection, leftType, rightType, joinType, rel.analyzeCondition(), nonEquiCondition);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteCorrelatedNestedLoopJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();

        assert rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.LEFT
                : CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG;

        SqlJoinProjection joinProjection = createJoinProjection(rel, outType, leftType.getFieldCount());

        assert joinProjection != null;

        SqlJoinPredicate joinPredicate = expressionFactory.joinPredicate(rel.getCondition(), outType, leftType.getFieldCount());
        BiPredicate<RowT, RowT> cond = (left, right) -> joinPredicate.test(ctx, left, right);

        RowFactory<RowT> rightRowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rightType));

        Node<RowT> node = new CorrelatedNestedLoopJoinNode<>(ctx, cond, rel.getVariablesSet(),
                rel.getCorrelationColumns(), rel.getJoinType(), rightRowFactory, joinProjection);

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

        SqlJoinProjection joinProjection = createJoinProjection(rel, rel.getRowType(), leftType.getFieldCount());

        ImmutableBitSet nullCompAsEqual = nullComparisonStrategyVector(rel, rel.analyzeCondition().leftSet());

        ImmutableIntList leftKeys = rel.leftCollation().getKeys();
        ImmutableIntList rightKeys = rel.rightCollation().getKeys();

        // Convert conditions to use collation indexes instead of field indexes.
        List<IntPair> conditionPairs = rel.analyzeCondition().pairs();
        List<IntPair> condIndexes = new ArrayList<>(conditionPairs.size());
        for (IntPair pair : conditionPairs) {
            condIndexes.add(IntPair.of(leftKeys.indexOf(pair.source), rightKeys.indexOf(pair.target)));
        }

        // Columns with larger indexes should go last.
        condIndexes.sort(CONDITION_PAIRS_COMPARATOR);

        int conditions = condIndexes.size();
        List<RelFieldCollation> leftCollation = new ArrayList<>(conditions);
        List<RelFieldCollation> rightCollation = new ArrayList<>(conditions);

        for (IntPair pair : condIndexes) {
            leftCollation.add(rel.leftCollation().getFieldCollations().get(pair.source));
            rightCollation.add(rel.rightCollation().getFieldCollations().get(pair.target));
        }

        if (IgniteUtils.assertionsEnabled()) {
            ensureComparatorCollationSatisfiesSourceCollation(leftCollation, leftKeys, "Left");
            ensureComparatorCollationSatisfiesSourceCollation(rightCollation, rightKeys, "Right");
        }

        SqlComparator sqlComparator = expressionFactory.comparator(
                leftCollation,
                rightCollation,
                nullCompAsEqual
        );
        Comparator<RowT> comp = (r1, r2) -> sqlComparator.compare(ctx, r1, r2);

        Node<RowT> node = MergeJoinNode.create(ctx, leftType, rightType, joinType, comp, joinProjection);

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
        ImmutableIntList requiredColumns = rel.requiredColumns();

        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);
        ScannableTable scannableTable = resolvedDependencies.scannableTable(tbl.id());

        IgniteIndex idx = tbl.indexes().get(rel.indexName());

        List<SearchBounds> searchBounds = rel.searchBounds();
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();

        Predicate<RowT> filters = null;
        if (condition != null) {
            SqlPredicate sqlPredicate = expressionFactory.predicate(condition, rowType);
            filters = row -> sqlPredicate.test(ctx, row);
        }

        Function<RowT, RowT> prj = null;
        if (projects != null) {
            SqlProjection sqlProjection = expressionFactory.project(projects, rowType);
            prj = row -> sqlProjection.project(ctx, row);
        }

        RangeIterable<RowT> ranges = null;

        if (searchBounds != null) {
            SqlComparator searchRowComparator = idx.type() == Type.SORTED
                    ? expressionFactory.comparator(IgniteIndex.createSearchRowCollation(idx.collation()))
                    : null;

            ranges = expressionFactory.ranges(searchBounds, idx.rowType(typeFactory, tbl.descriptor()), searchRowComparator).get(ctx);
        }

        RelCollation collation = rel.collation();

        ColocationGroup group = ctx.group(rel.sourceId());

        assert group != null;

        Comparator<RowT> comp = null;
        if (idx.type() == Type.SORTED && collation != null && !nullOrEmpty(collation.getFieldCollations())) {
            // Collation returned by rel is mapped according to projection merged into the rel. But we need
            // comparator to merge streams of different partition. These streams respect `requiredColumns`,
            // but projection is happened on later stage of execution, therefor if projection exists, we need
            // to rebuild collation prior to create comparator.
            RelCollation partitionStreamCollation;

            if (projects != null) {
                partitionStreamCollation = idx.collation();

                if (rel.requiredColumns() != null) {
                    Mappings.TargetMapping mapping = createMapping(
                            null,
                            rel.requiredColumns(),
                            tbl.getRowType(typeFactory).getFieldCount()
                    );

                    partitionStreamCollation = partitionStreamCollation.apply(mapping);
                }
            } else {
                partitionStreamCollation = collation;
            }

            SqlComparator searchRowComparator = expressionFactory.comparator(partitionStreamCollation);

            comp = (r1, r2) -> searchRowComparator.compare(ctx, r1, r2);

        }

        if (!group.nodeNames().contains(ctx.localNode().name())) {
            return new ScanNode<>(ctx, Collections.emptyList());
        }

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));
        PartitionProvider<RowT> partitionProvider = ctx.getPartitionProvider(rel.sourceId(), group, tbl);

        return new IndexScanNode<>(
                ctx,
                rowFactory,
                idx,
                scannableTable,
                tbl.descriptor(),
                partitionProvider,
                comp,
                ranges,
                filters,
                prj,
                requiredColumns
        );
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableScan rel) {
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();
        ImmutableIntList requiredColumns = rel.requiredColumns();

        IgniteTable tbl = rel.getTable().unwrapOrThrow(IgniteTable.class);
        ScannableTable scannableTable = resolvedDependencies.scannableTable(tbl.id());

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);

        Predicate<RowT> filters = null;
        if (condition != null) {
            SqlPredicate sqlPredicate = expressionFactory.predicate(condition, rowType);
            filters = row -> sqlPredicate.test(ctx, row);
        }

        Function<RowT, RowT> prj = null;
        if (projects != null) {
            SqlProjection sqlProjection = expressionFactory.project(projects, rowType);
            prj = row -> sqlProjection.project(ctx, row);
        }

        long sourceId = rel.sourceId();
        ColocationGroup group = ctx.group(sourceId);

        assert group != null;

        if (!group.nodeNames().contains(ctx.localNode().name())) {
            return new ScanNode<>(ctx, Collections.emptyList());
        }

        // TODO: IGNITE-22822 fix required columns.
        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));

        PartitionProvider<RowT> partitionProvider = ctx.getPartitionProvider(rel.sourceId(), group, tbl);

        return new TableScanNode<>(
                ctx,
                rowFactory,
                tbl,
                scannableTable,
                partitionProvider,
                filters,
                prj,
                requiredColumns
        );
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSystemViewScan rel) {
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();
        ImmutableIntList requiredColumns = rel.requiredColumns();
        IgniteDataSource igniteDataSource = rel.getTable().unwrapOrThrow(IgniteDataSource.class);

        BinaryTupleSchema schema = fromTableDescriptor(igniteDataSource.descriptor());

        ScannableDataSource dataSource = resolvedDependencies.dataSource(igniteDataSource.id());

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = igniteDataSource.getRowType(typeFactory, requiredColumns);

        Predicate<RowT> filters = null;
        if (condition != null) {
            SqlPredicate sqlPredicate = expressionFactory.predicate(condition, rowType);
            filters = row -> sqlPredicate.test(ctx, row);
        }

        Function<RowT, RowT> prj = null;
        if (projects != null) {
            SqlProjection sqlProjection = expressionFactory.project(projects, rowType);
            prj = row -> sqlProjection.project(ctx, row);
        }

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));
        return new DataSourceScanNode<>(
                ctx,
                rowFactory,
                schema,
                dataSource,
                filters,
                prj,
                requiredColumns
        );
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteValues rel) {
        List<List<RexLiteral>> vals = cast(rel.getTuples());

        List<RowT> rows = new ArrayList<>(vals.size());
        for (List<RexLiteral> literals : vals) {
            rows.add(expressionFactory.rowSource(cast(literals)).get(ctx));
        }

        return new ScanNode<>(ctx, rows);
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
        long offset = rel.offset() == null ? 0 : validateAndGetFetchOffsetParams(rel.offset(), "offset");
        long fetch = rel.fetch() == null ? -1 : validateAndGetFetchOffsetParams(rel.fetch(), "fetch");

        LimitNode<RowT> node = new LimitNode<>(ctx, offset, fetch);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSort rel) {
        RelCollation collation = rel.getCollation();

        long offset = rel.offset == null ? 0 : validateAndGetFetchOffsetParams(rel.offset, "offset");
        long fetch = rel.fetch == null ? -1 : validateAndGetFetchOffsetParams(rel.fetch, "fetch");

        SqlComparator sqlComparator = expressionFactory.comparator(collation);
        SortNode<RowT> node = new SortNode<>(
                ctx,
                (r1, r2) -> sqlComparator.compare(ctx, r1, r2),
                offset,
                fetch
        );

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

        SqlPredicate sqlPredicate = expressionFactory.predicate(rel.condition(), rel.getRowType());
        Predicate<RowT> filter = row -> sqlPredicate.test(ctx, row);
        SqlComparator comparator = expressionFactory.comparator(collation);
        RangeIterable<RowT> ranges = expressionFactory.ranges(rel.searchBounds(), rel.getRowType(), comparator).get(ctx);

        IndexSpoolNode<RowT> node = IndexSpoolNode.createTreeSpool(
                ctx,
                rel.getRowType(),
                collation,
                (r1, r2) -> comparator.compare(ctx, r1, r2),
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
        SqlRowProvider rowProvider = expressionFactory.rowSource(rel.searchRow());
        Supplier<RowT> searchRow = () -> rowProvider.get(ctx);

        SqlPredicate sqlPredicate = expressionFactory.predicate(rel.condition(), rel.getRowType());
        Predicate<RowT> filter = row -> sqlPredicate.test(ctx, row);

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

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));

        List<Node<RowT>> inputs = Commons.transform(rel.getInputs(), this::visit);
        int columnNum;

        if (rel instanceof IgniteMapSetOp) {
            columnNum = rel.getInput(0).getRowType().getFieldCount();
        } else {
            columnNum = rowType.getFieldCount();
        }

        AbstractSetOpNode<RowT> node;

        if (rel instanceof Minus) {
            node = new MinusNode<>(ctx, columnNum, rel.aggregateType(), rel.all(), rowFactory);
        } else if (rel instanceof IgniteIntersect) {
            int inputsNum;

            if (rel instanceof IgniteReduceIntersect) {
                // MAP phase of intersect operator produces (c1, c2, .., cN, counters_input1, ... counters_inputM),
                // so the number of input relations is equal to the number of input cols to reduce phase
                // minus the number of output columns produced by a set operator (See IgniteMapSetOp::buildRowType).
                int inputCols = rel.getInput(0).getRowType().getFieldCount();
                int outputCols = rel.getRowType().getFieldCount();

                inputsNum = inputCols - outputCols;
            } else {
                inputsNum = rel.getInputs().size();
            }

            node = new IntersectNode<>(ctx, columnNum, rel.aggregateType(), rel.all(), rowFactory, inputsNum);
        } else {
            throw new AssertionError("Unexpected set node: " + rel);
        }

        node.register(inputs);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableFunctionScan rel) {
        TableFunction<RowT> tableFunction = tableFunctionRegistry.getTableFunction(ctx, (RexCall) rel.getCall());

        return new ScanNode<>(ctx, tableFunction);
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableModify rel) {
        IgniteTable table = rel.getTable().unwrapOrThrow(IgniteTable.class);
        UpdatableTable updatableTable = resolvedDependencies.updatableTable(table.id());
        RelDataType rowType = rel.getInput().getRowType();

        ModifyNode<RowT> node = new ModifyNode<>(
                ctx, updatableTable, rel.sourceId(), rel.getOperation(), rel.getUpdateColumnList(), convertStructuredType(rowType)
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReceiver rel) {
        RelDataType rowType = rel.getRowType();

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));

        RelCollation collation = rel.collation();
        Comparator<RowT> comp = null;
        if (collation != null && !nullOrEmpty(collation.getFieldCollations())) {
            SqlComparator searchRowComparator = expressionFactory.comparator(collation);

            comp = (r1, r2) -> searchRowComparator.compare(ctx, r1, r2);
        }

        Inbox<RowT> inbox = new Inbox<>(ctx, exchangeSvc, mailboxRegistry,
                ctx.remotes(rel.exchangeId()), comp,
                rowFactory, rel.exchangeId(), rel.sourceFragmentId());

        mailboxRegistry.register(inbox);

        return inbox;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteColocatedHashAggregate rel) {
        AggregateType type = AggregateType.SINGLE;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggCallList().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.<RowT>accumulatorsFactory(
                    type, rel.getAggCallList(), inputType
            ).get(ctx);
        }

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, type, rel.getGroupSets(), accumulators, rowFactory);

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

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggCallList().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.<RowT>accumulatorsFactory(
                    type, rel.getAggCallList(), inputType
            ).get(ctx);
        }

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, type, rel.getGroupSets(), accumulators, rowFactory);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReduceHashAggregate rel) {
        AggregateType type = AggregateType.REDUCE;

        RelDataType rowType = rel.getRowType();

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggregateCalls().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.<RowT>accumulatorsFactory(
                    type, rel.getAggregateCalls(), rel.getInput().getRowType()
            ).get(ctx);
        }

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, type, rel.getGroupSets(), accumulators, rowFactory);

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

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggCallList().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.<RowT>accumulatorsFactory(
                    type, rel.getAggCallList(), inputType
            ).get(ctx);
        }

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));

        RelCollation collation = rel.collation();
        Comparator<RowT> comp = null;
        if (collation != null && !nullOrEmpty(collation.getFieldCollations())) {
            SqlComparator searchRowComparator = expressionFactory.comparator(collation);

            comp = (r1, r2) -> searchRowComparator.compare(ctx, r1, r2);
        }

        if (rel.getGroupSet().isEmpty() && comp == null) {
            comp = (k1, k2) -> 0;
        }

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                type,
                rel.getGroupSet(),
                accumulators,
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

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggCallList().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.<RowT>accumulatorsFactory(
                    type, rel.getAggCallList(), inputType
            ).get(ctx);
        }

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));

        RelCollation collation = rel.collation();
        Comparator<RowT> comp = null;
        if (collation != null && !nullOrEmpty(collation.getFieldCollations())) {
            SqlComparator searchRowComparator = expressionFactory.comparator(collation);

            comp = (r1, r2) -> searchRowComparator.compare(ctx, r1, r2);
        }

        if (rel.getGroupSet().isEmpty() && comp == null) {
            comp = (k1, k2) -> 0;
        }

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                type,
                rel.getGroupSet(),
                accumulators,
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

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggregateCalls().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.<RowT>accumulatorsFactory(
                    type, rel.getAggregateCalls(), rel.getInput().getRowType()
            ).get(ctx);
        }

        RowFactory<RowT> rowFactory = ctx.rowFactoryFactory().create(convertStructuredType(rowType));

        RelCollation collation = rel.collation();
        Comparator<RowT> comp = null;
        if (collation != null && !nullOrEmpty(collation.getFieldCollations())) {
            SqlComparator searchRowComparator = expressionFactory.comparator(collation);

            comp = (r1, r2) -> searchRowComparator.compare(ctx, r1, r2);
        }

        if (rel.getGroupSet().isEmpty() && comp == null) {
            comp = (k1, k2) -> 0;
        }

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                type,
                rel.getGroupSet(),
                accumulators,
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
        throw new AssertionError(rel.getClass());
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteKeyValueGet rel) {
        throw new AssertionError(rel.getClass());
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteKeyValueModify rel) {
        throw new AssertionError(rel.getClass());
    }

    @Override
    public Node<RowT> visit(IgniteSelectCount rel) {
        throw new AssertionError(rel.getClass());
    }

    private Node<RowT> visit(RelNode rel) {
        return visit((IgniteRel) rel);
    }

    public <T extends Node<RowT>> T go(IgniteRel rel) {
        return (T) visit(rel);
    }

    private static BinaryTupleSchema fromTableDescriptor(TableDescriptor descriptor) {
        Element[] elements = new Element[descriptor.columnsCount()];

        int idx = 0;
        for (ColumnDescriptor column : descriptor) {
            elements[idx++] = new Element(column.physicalType(), column.nullable());
        }

        return BinaryTupleSchema.create(elements);
    }

    private static boolean canFuseProjectionInto(RelNode rel) {
        if (rel instanceof Join) {
            Join join = (Join) rel;

            return JOIN_NEEDS_PROJECTION.contains(join.getJoinType());
        }

        return false;
    }

    private @Nullable SqlJoinProjection createJoinProjection(Join rel, RelDataType outType, int leftRowSize) {
        SqlJoinProjection joinProjection = null;
        if (projectionToFuse != null) {
            assert JOIN_NEEDS_PROJECTION.contains(rel.getJoinType());

            joinProjection = expressionFactory.joinProject(projectionToFuse, outType, leftRowSize);

            projectionToFuse = null;
        } else if (JOIN_NEEDS_PROJECTION.contains(rel.getJoinType())) {
            List<RexNode> identityProjection = rel.getCluster().getRexBuilder().identityProjects(outType);

            joinProjection = expressionFactory.joinProject(identityProjection, outType, leftRowSize);
        }

        return joinProjection;
    }

    private long validateAndGetFetchOffsetParams(RexNode node, String op) {
        SqlScalar<Number> sqlScalar = expressionFactory.scalar(node);
        Number param = sqlScalar.get(ctx);

        long paramAsLong;

        try {
            paramAsLong = IgniteMath.convertToLongExact(param);
        } catch (RuntimeException ex) {
            throw new SqlException(Sql.STMT_VALIDATION_ERR, IgniteResource.INSTANCE.illegalFetchLimit(op).str(), ex);
        }

        if (paramAsLong < 0) {
            throw new SqlException(Sql.STMT_VALIDATION_ERR, IgniteResource.INSTANCE.illegalFetchLimit(op).str());
        }

        return paramAsLong;
    }

    /**
     * Evaluate a null comparison strategy as a bitset for the case of NOT DISTINCT FROM syntax.
     */
    private static ImmutableBitSet nullComparisonStrategyVector(IgniteMergeJoin rel, ImmutableBitSet leftKeys) {
        ImmutableBitSet.Builder nullCompAsEqualBuilder = ImmutableBitSet.builder();

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

        List<RexNode> conjunctions = RelOptUtil.conjunctions(rel.getCondition());
        for (RexNode expr : conjunctions) {
            if (expr.getKind() == SqlKind.IS_NOT_DISTINCT_FROM) {
                shuttle.apply(expr);
            }
        }

        return nullCompAsEqualBuilder.build();
    }

    private static void ensureComparatorCollationSatisfiesSourceCollation(
            List<RelFieldCollation> compCollation,
            ImmutableIntList collationKeys,
            String name
    ) {
        int[] effectiveCollation = compCollation.stream().mapToInt(RelFieldCollation::getFieldIndex).distinct().toArray();

        assert effectiveCollation.length <= collationKeys.size() : name + " effective collation size mismatch";

        int[] keysPrefix = collationKeys.stream().mapToInt(Integer::intValue).limit(effectiveCollation.length).toArray();

        assert Arrays.equals(effectiveCollation, keysPrefix) : name + " collation mismatch the source collation";
    }
}
