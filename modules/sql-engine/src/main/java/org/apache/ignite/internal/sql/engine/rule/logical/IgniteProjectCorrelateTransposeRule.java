package org.apache.ignite.internal.sql.engine.rule.logical;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.ProjectCorrelateTransposeRule;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rule.logical.IgniteProjectCorrelateTransposeRule.IgniteProjectCorrelateTransposeRuleConfig;
import org.immutables.value.Value;

/**
 * Planner rule that pushes a {@link Project} under {@link Correlate} to apply
 * on Correlate's left and right inputs.
 *
 * <p>There are two differences between this rule and {@link ProjectCorrelateTransposeRule}:
 * 1. is that former take into account RexCall parameters for functions in LogicalTableFunctionScan like a SYSTEM_RANGE.
 * 2. For some reasons for rewritten node can try be rewritten again, so for that case we just skip the rewrite.
 *
 * @see CoreRules#PROJECT_CORRELATE_TRANSPOSE
 */
@Value.Enclosing
public class IgniteProjectCorrelateTransposeRule
        extends RelRule<IgniteProjectCorrelateTransposeRuleConfig>
        implements TransformationRule {
    public static final RelOptRule INSTANCE = IgniteProjectCorrelateTransposeRuleConfig.DEFAULT.toRule();


    /** Creates a ProjectCorrelateTransposeRule. */
    protected IgniteProjectCorrelateTransposeRule(IgniteProjectCorrelateTransposeRuleConfig config) {
        super(config);
    }
    //~ Methods ----------------------------------------------------------------

    @Override public void onMatch(RelOptRuleCall call) {
        Project origProject = call.rel(0);
        Correlate correlate = call.rel(1);

        // locate all fields referenced in the projection
        // determine which inputs are referenced in the projection;
        // if all fields are being referenced and there are no
        // special expressions, no point in proceeding any further
        PushProjector pushProjector =
                new PushProjector(origProject, call.builder().literal(true), correlate,
                        config.preserveExprCondition(), call.builder());
        if (pushProjector.locateAllRefs()) {
            return;
        }

        // create left and right projections, projecting only those
        // fields referenced on each side
        RelNode leftProject =
                pushProjector.createProjectRefsAndExprs(
                        correlate.getLeft(),
                        true,
                        false);
        RelNode rightProject =
                pushProjector.createProjectRefsAndExprs(
                        correlate.getRight(),
                        true,
                        true);

        Map<Integer, Integer> requiredColsMap = new HashMap<>();

        // adjust requiredColumns that reference the projected columns
        int[] adjustments = pushProjector.getAdjustments();
        BitSet updatedBits = new BitSet();
        for (Integer col : correlate.getRequiredColumns()) {
            int newCol = col + adjustments[col];
            updatedBits.set(newCol);
            requiredColsMap.put(col, newCol);
        }

        RexBuilder rexBuilder = call.builder().getRexBuilder();

        CorrelationId correlationId = correlate.getCluster().createCorrel();
        RexCorrelVariable rexCorrel =
                (RexCorrelVariable) rexBuilder.makeCorrel(
                        leftProject.getRowType(),
                        correlationId);

        // updates RexCorrelVariable and sets actual RelDataType for RexFieldAccess
        rightProject =
                rightProject.accept(
                        new IgniteProjectCorrelateTransposeRule.RelNodesExprsHandler(
                                new IgniteProjectCorrelateTransposeRule.RexFieldAccessReplacer(correlate.getCorrelationId(),
                                        rexCorrel, rexBuilder, requiredColsMap)));

        // create a new correlate with the projected children
        Correlate newCorrelate =
                correlate.copy(
                        correlate.getTraitSet(),
                        leftProject,
                        rightProject,
                        correlationId,
                        ImmutableBitSet.of(BitSets.toIter(updatedBits)),
                        correlate.getJoinType());

        // put the original project on top of the correlate, converting it to
        // reference the modified projection list
        RelNode topProject =
                pushProjector.createNewProject(newCorrelate, adjustments);

        call.transformTo(topProject);
    }

    /**
     * Visitor for RexNodes which replaces {@link RexCorrelVariable} with specified.
     */
    public static class RexFieldAccessReplacer extends RexShuttle {
        private final RexBuilder builder;
        private final CorrelationId rexCorrelVariableToReplace;
        private final RexCorrelVariable rexCorrelVariable;
        private final Map<Integer, Integer> requiredColsMap;

        public RexFieldAccessReplacer(
                CorrelationId rexCorrelVariableToReplace,
                RexCorrelVariable rexCorrelVariable,
                RexBuilder builder,
                Map<Integer, Integer> requiredColsMap) {
            this.rexCorrelVariableToReplace = rexCorrelVariableToReplace;
            this.rexCorrelVariable = rexCorrelVariable;
            this.builder = builder;
            this.requiredColsMap = requiredColsMap;
        }

        @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
            if (variable.id.equals(rexCorrelVariableToReplace)) {
                return rexCorrelVariable;
            }
            return variable;
        }

        @Override public RexNode visitCall(RexCall call) {
            // Over expression should not be pushed.
            return super.visitCall(call);
        }

        @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            RexNode refExpr = fieldAccess.getReferenceExpr().accept(this);
            // creates new RexFieldAccess instance for the case when referenceExpr was replaced.
            // Otherwise calls super method.
            if (refExpr == rexCorrelVariable) {
                int fieldIndex = fieldAccess.getField().getIndex();
                Integer idx = requiredColsMap.get(fieldIndex);
                if(idx == null){
                    // "no entry for field in requiredColsMap
                    // Seems we got already rewritten node, so just skip it.
                    return super.visitFieldAccess(fieldAccess);
                }
                return builder.makeFieldAccess(refExpr, idx);
            }
            return super.visitFieldAccess(fieldAccess);
        }
    }

    /**
     * Visitor for RelNodes which applies specified {@link RexShuttle} visitor
     * for every node in the tree.
     */
    public static class RelNodesExprsHandler extends RelShuttleImpl {
        private final RexShuttle rexVisitor;

        public RelNodesExprsHandler(RexShuttle rexVisitor) {
            this.rexVisitor = rexVisitor;
        }

        @Override protected RelNode visitChild(RelNode parent, int i,
                RelNode input) {
            return super.visitChild(parent, i, input.stripped())
                    .accept(rexVisitor);
        }

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof LogicalTableFunctionScan) {
                List<RexNode> operands = ((RexCall) ((LogicalTableFunctionScan) other).getCall()).getOperands();
                for (int i = 0; i < operands.size(); i++) {
                    operands.get(i).accept(rexVisitor);
                }
            }
            return super.visit(other).accept(rexVisitor);
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface IgniteProjectCorrelateTransposeRuleConfig extends RelRule.Config {
        IgniteProjectCorrelateTransposeRuleConfig DEFAULT = ImmutableIgniteProjectCorrelateTransposeRule
                .IgniteProjectCorrelateTransposeRuleConfig.builder()
                .withPreserveExprCondition(expr -> !(expr instanceof RexOver))
                .build()
                .withOperandFor(Project.class, Correlate.class);

        @Override default IgniteProjectCorrelateTransposeRule toRule() {
            return new IgniteProjectCorrelateTransposeRule(this);
        }

        /** Defines when an expression should not be pushed. */
        PushProjector.ExprCondition preserveExprCondition();

        /** Sets {@link #preserveExprCondition()}. */
        IgniteProjectCorrelateTransposeRuleConfig withPreserveExprCondition(PushProjector.ExprCondition condition);

        /** Defines an operand tree for the given classes. */
        default IgniteProjectCorrelateTransposeRuleConfig withOperandFor(Class<? extends Project> projectClass,
                Class<? extends Correlate> correlateClass) {
            return withOperandSupplier(b0 ->
                    b0.operand(projectClass).oneInput(b1 ->
                            b1.operand(correlateClass).anyInputs()))
                    .as(IgniteProjectCorrelateTransposeRuleConfig.class);
        }
    }
}