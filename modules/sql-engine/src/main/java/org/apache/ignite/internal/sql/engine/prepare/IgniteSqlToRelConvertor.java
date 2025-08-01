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

import static java.util.Objects.requireNonNull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.jetbrains.annotations.Nullable;

/** Converts a SQL parse tree into a relational algebra operators. */
public class IgniteSqlToRelConvertor extends SqlToRelConverter implements InitializerContext {
    private final Deque<SqlCall> datasetStack = new ArrayDeque<>();

    private RelBuilder relBuilder;

    IgniteSqlToRelConvertor(
            RelOptTable.ViewExpander viewExpander,
            @Nullable SqlValidator validator,
            Prepare.CatalogReader catalogReader, RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config cfg
    ) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, cfg);

        relBuilder = config.getRelBuilderFactory().create(cluster, null);
    }

    /** {@inheritDoc} */
    @Override protected RelRoot convertQueryRecursive(SqlNode qry, boolean top, @Nullable RelDataType targetRowType) {
        if (qry.getKind() == SqlKind.MERGE) {
            return RelRoot.of(convertMerge((SqlMerge) qry), qry.getKind());
        } else {
            return super.convertQueryRecursive(qry, top, targetRowType);
        }
    }

    @Override protected RelNode convertInsert(SqlInsert call) {
        datasetStack.push(call);

        RelNode rel = super.convertInsert(call);

        datasetStack.pop();

        return rel;
    }

    @Override
    public SqlNode validateExpression(RelDataType rowType, SqlNode expr) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private static class DefaultChecker extends SqlShuttle {
        private boolean hasDefaults(SqlCall call) {
            try {
                call.accept(this);
                return false;
            } catch (ControlFlowException e) {
                return true;
            }
        }

        @Override public @Nullable SqlNode visit(SqlCall call) {
            if (call.getKind() == SqlKind.DEFAULT) {
                throw new ControlFlowException();
            }

            return super.visit(call);
        }
    }

    @Override public RelNode convertValues(SqlCall values, RelDataType targetRowType) {
        DefaultChecker checker = new DefaultChecker();

        boolean hasDefaults = checker.hasDefaults(values);

        if (hasDefaults) {
            SqlValidatorScope scope = validator.getOverScope(values);
            assert scope != null;
            Blackboard bb = createBlackboard(scope, null, false);

            convertValuesImplEx(bb, values, targetRowType);
            return bb.root();
        } else {
            // a bit lightweight than default processing one.
            return super.convertValues(values, targetRowType);
        }
    }

    private void convertValuesImplEx(Blackboard bb, SqlCall values, RelDataType targetRowType) {
        SqlCall insertOp = datasetStack.peek();
        assert insertOp instanceof SqlInsert;
        assert values == ((SqlInsert) insertOp).getSource();
        RelOptTable targetTable = getTargetTable(insertOp);
        assert targetTable != null;

        IgniteDataSource ignTable = targetTable.unwrap(IgniteDataSource.class);

        List<RelDataTypeField> tblFields = targetTable.getRowType().getFieldList();
        List<String> targetFields = targetRowType.getFieldNames();

        int[] mapping = new int[targetFields.size()];

        int pos = 0;

        for (String fld : targetFields) {
            int tblPos = 0;
            for (RelDataTypeField tblFld : tblFields) {
                if (tblFld.getName().equals(fld)) {
                    mapping[pos++] = tblPos;
                    break;
                }
                ++tblPos;
            }
        }

        for (SqlNode rowConstructor : values.getOperandList()) {
            SqlCall rowConstructor0 = (SqlCall) rowConstructor;

            List<Pair<RexNode, String>> exps = new ArrayList<>(targetFields.size());

            pos = 0;
            for (; pos < targetFields.size(); ++pos) {
                SqlNode operand = rowConstructor0.getOperandList().get(pos);

                if (operand.getKind() == SqlKind.DEFAULT) {
                    RexNode def = ignTable.descriptor().newColumnDefaultValue(targetTable, mapping[pos], bb);

                    exps.add(Pair.of(def, SqlValidatorUtil.alias(operand, pos)));
                } else {
                    exps.add(Pair.of(bb.convertExpression(operand), SqlValidatorUtil.alias(operand, pos)));
                }
            }

            RelNode in = (null == bb.root) ? LogicalValues.createOneRow(cluster) : bb.root;

            relBuilder.push(in)
                    .project(Pair.left(exps), Pair.right(exps));
        }

        bb.setRoot(
                relBuilder.union(true, values.getOperandList().size())
                        .build(), true);
    }

    /**
     * This method was copy-pasted from super-method except this changes:
     * - For updateCall we require all columns in the project and should not skip anything.
     * - If there is no updateCall, LEFT JOIN converted to ANTI JOIN.
     */
    private RelNode convertMerge(SqlMerge call) {
        RelOptTable targetTable = getTargetTable(call);

        // convert update column list from SqlIdentifier to String
        final List<String> targetColumnNameList = new ArrayList<>();
        final RelDataType targetRowType = targetTable.getRowType();
        SqlUpdate updateCall = call.getUpdateCall();
        if (updateCall != null) {
            for (SqlNode targetColumn : updateCall.getTargetColumnList()) {
                SqlIdentifier id = (SqlIdentifier) targetColumn;
                RelDataTypeField field =
                        SqlValidatorUtil.getTargetField(
                                targetRowType, typeFactory, id, catalogReader, targetTable);
                assert field != null : "column " + id.toString() + " not found";
                targetColumnNameList.add(field.getName());
            }
        }

        // replace the projection of the source select with a
        // projection that contains the following:
        // 1) the expressions corresponding to the new insert row (if there is
        //    an insert)
        // 2) all columns from the target table (if there is an update)
        // 3) the set expressions in the update call (if there is an update)

        // first, convert the merge's source select to construct the columns
        // from the target table and the set expressions in the update call
        RelNode mergeSourceRel = convertSelect(
                requireNonNull(call.getSourceSelect(), () -> "sourceSelect for " + call), false);

        // then, convert the insert statement so we can get the insert
        // values expressions
        SqlInsert insertCall = call.getInsertCall();
        int numLevel1Exprs = 0;
        List<RexNode> level1InsertExprs = null;
        List<RexNode> level2InsertExprs = null;
        boolean needRepairProject = false;
        if (insertCall != null) {
            RelNode insertRel = convertInsert(insertCall);

            // if there are 2 level of projections in the insert source, combine
            // them into a single project; level1 refers to the topmost project;
            // the level1 projection contains references to the level2
            // expressions, except in the case where no target expression was
            // provided, in which case, the expression is the default value for
            // the column; or if the expressions directly map to the source
            // table
            RelNode input = insertRel.getInput(0);

            if (input instanceof LogicalProject) {
                level1InsertExprs = ((LogicalProject) input).getProjects();
            } else {
                // TODO https://issues.apache.org/jira/browse/IGNITE-22293
                // convertInsert() may return LogicalTableModify without projection in the input.
                // As a workaround for this case, we additionally build required column expressions.
                RelDataType rowType = input.getRowType();
                level1InsertExprs = new ArrayList<>(rowType.getFieldCount());
                int pos = 0;
                for (RelDataTypeField type : rowType.getFieldList()) {
                    level1InsertExprs.add(rexBuilder.makeInputRef(type.getType(), pos++));
                }
            }

            numLevel1Exprs = level1InsertExprs.size();

            if (!input.getInputs().isEmpty() && input.getInput(0) instanceof LogicalProject) {
                level2InsertExprs = ((LogicalProject) input.getInput(0)).getProjects();
            }

            // If source rel contains project, then we expect at least 3 nested projects,
            // otherwise it means source rel project was merged unexpectedly and project must be repaired.
            needRepairProject = ((LogicalJoin) mergeSourceRel.getInput(0)).getLeft() instanceof LogicalProject
                    && (input.getInputs().isEmpty()
                    || !(input.getInput(0) instanceof LogicalProject)
                    || input.getInput(0).getInputs().isEmpty()
                    || !(input.getInput(0).getInput(0) instanceof LogicalProject));
        }

        LogicalJoin join = (LogicalJoin) mergeSourceRel.getInput(0);

        List<RexNode> projects = new ArrayList<>();

        for (int level1Idx = 0; level1Idx < numLevel1Exprs; level1Idx++) {
            requireNonNull(level1InsertExprs, "level1InsertExprs");
            if ((level2InsertExprs != null)
                    && (level1InsertExprs.get(level1Idx) instanceof RexInputRef)) {
                int level2Idx =
                        ((RexInputRef) level1InsertExprs.get(level1Idx)).getIndex();
                projects.add(level2InsertExprs.get(level2Idx));
            } else {
                projects.add(level1InsertExprs.get(level1Idx));
            }
        }

        // It is possible the method `convertInsert` merge projections (e.g. due to RelBuilder.Config.withBloat)
        // In that case, we should recover project on top of source project (mergeSourceRel left branch) to get correct input refs.
        // Most likely, we should disable bloat, but the `relBuilder` is out of our control, due parent private field visibility.
        if (needRepairProject) {
            projects = repairProject(join, projects);
        }

        if (updateCall != null) {
            final LogicalProject project = (LogicalProject) mergeSourceRel;
            projects.addAll(project.getProjects());
        } else {
            // Convert to ANTI join if there is no UPDATE clause.
            join = join.copy(join.getTraitSet(), join.getCondition(), join.getLeft(), join.getRight(), JoinRelType.ANTI,
                    false);
        }

        RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null)
                .transform(config.getRelBuilderConfigTransform());

        relBuilder.push(join)
                .project(projects);

        return LogicalTableModify.create(targetTable, catalogReader,
                relBuilder.build(), LogicalTableModify.Operation.MERGE,
                targetColumnNameList, null, false);
    }

    /**
     * This is a dirty hack to fix merged InsertCall projection.
     */
    private List<RexNode> repairProject(LogicalJoin join, List<RexNode> actual) {
        if (!(join.getLeft() instanceof LogicalProject)) {
            return actual;
        }

        List<RexNode> original = ((LogicalProject) join.getLeft()).getProjects();

        ArrayList<RexNode> recovered = new ArrayList<>(actual.size());
        for (RexNode rexNode : actual) {
            int index = original.indexOf(rexNode);
            if (index == -1) {
                recovered.add(rexNode);
            } else {
                recovered.add(rexBuilder.makeInputRef(rexNode.getType(), index));
            }
        }

        return recovered;
    }

    @Override
    public RelOptTable getTargetTable(SqlNode call) {
        return super.getTargetTable(call);
    }
}
