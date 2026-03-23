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

package org.apache.ignite.internal.sql.engine.rule.logical;

import static org.apache.calcite.util.Util.last;
import static org.apache.ignite.internal.sql.engine.hint.IgniteHint.FORCE_INDEX;
import static org.apache.ignite.internal.sql.engine.hint.IgniteHint.NO_INDEX;
import static org.apache.ignite.internal.util.IgniteUtils.capacity;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.hint.IgniteHint;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.AbstractIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.HintUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.SqlException;
import org.immutables.value.Value;

/**
 * ExposeIndexRule.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@Value.Enclosing
public class ExposeIndexRule extends RelRule<ExposeIndexRule.Config> {
    public static final RelOptRule INSTANCE = Config.DEFAULT.withDescription("ExposeIndexRule").toRule();

    private static final EnumSet<IgniteHint> INDEX_HINTS = EnumSet.of(NO_INDEX, FORCE_INDEX);

    private ExposeIndexRule(Config config) {
        super(config);
    }

    private static boolean preMatch(IgniteLogicalTableScan scan) {
        // has indexes to expose
        return !scan.getTable().unwrap(IgniteTable.class).indexes().isEmpty();
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        IgniteLogicalTableScan scan = call.rel(0);
        RelOptCluster cluster = scan.getCluster();

        RelOptTable optTable = scan.getTable();
        IgniteTable igniteTable = optTable.unwrap(IgniteTable.class);
        List<String> names = scan.fieldNames();
        List<RexNode> proj = scan.projects();
        RexNode condition = scan.condition();
        ImmutableIntList requiredCols = scan.requiredColumns();

        List<IgniteLogicalIndexScan> indexes = igniteTable.indexes().values().stream()
                .map(idx -> idx.toRel(cluster, optTable, names, proj, condition, requiredCols))
                .filter(idx -> filter(igniteTable, idx.indexName(), idx.searchBounds()))
                .collect(Collectors.toList());

        if (indexes.isEmpty()) {
            return;
        }

        indexes = applyHints(scan, indexes);

        if (indexes.isEmpty()) {
            return;
        }

        Map<RelNode, RelNode> equivMap = IgniteUtils.newHashMap(indexes.size());
        for (int i = 1; i < indexes.size(); i++) {
            equivMap.put(indexes.get(i), scan);
        }

        call.transformTo(indexes.get(0), equivMap);
    }

    /** Filter actual indexes list and prune-table-scan flag if any index is forced to use. */
    private static List<IgniteLogicalIndexScan> applyHints(TableScan scan, List<IgniteLogicalIndexScan> indexes) {
        List<RelHint> hints = HintUtils.hints(scan, INDEX_HINTS);

        if (hints.isEmpty()) {
            return indexes;
        }

        Set<String> tblIdxNames = indexes.stream().map(AbstractIndexScan::indexName).collect(Collectors.toSet());
        Set<String> idxToSkip = new HashSet<>(capacity(tblIdxNames.size()));
        Set<String> idxToUse = new HashSet<>(capacity(tblIdxNames.size()));

        for (RelHint hint : hints) {
            Collection<String> hintIdxNames = hint.listOptions;
            boolean noIndex = hint.hintName.equals(NO_INDEX.name());

            if (hintIdxNames.isEmpty()) {
                if (noIndex) {
                    if (!idxToUse.isEmpty()) {
                        throw new SqlException(STMT_VALIDATION_ERR,
                                "Indexes " + idxToUse + " of table '" + last(scan.getTable().getQualifiedName())
                                        + "' has already been forced to use by other hints before.");
                    }

                    idxToSkip.addAll(tblIdxNames);
                }

                continue;
            }

            for (String hintIdxName : hintIdxNames) {
                if (!tblIdxNames.contains(hintIdxName)) {
                    continue;
                }

                if (noIndex) {
                    if (idxToUse.contains(hintIdxName)) {
                        throw new SqlException(STMT_VALIDATION_ERR, "Index '" + hintIdxName + " of table '"
                                + last(scan.getTable().getQualifiedName()) + "' has already been forced in other hints.");
                    }

                    idxToSkip.add(hintIdxName);
                } else {
                    if (idxToSkip.contains(hintIdxName)) {
                        throw new SqlException(STMT_VALIDATION_ERR, "Index '" + hintIdxName + "' of table '"
                                + last(scan.getTable().getQualifiedName()) + "' has already been disabled in other hints.");
                    }

                    idxToUse.add(hintIdxName);
                }
            }
        }

        if (!idxToUse.isEmpty()) {
            scan.getCluster().getPlanner().prune(scan);
        }

        return indexes.stream()
                .filter(idx -> !idxToSkip.contains(idx.indexName()) && (idxToUse.isEmpty() || idxToUse.contains(idx.indexName())))
                .collect(Collectors.toList());
    }

    /** Filter pre known not applicable variants. Significant shrink search space in some cases. */
    private static boolean filter(IgniteTable table, String idxName, List<SearchBounds> searchBounds) {
        IgniteIndex index = table.indexes().get(idxName);

        return index.type() == Type.SORTED || (searchBounds != null
                && searchBounds.stream().noneMatch(bound -> bound.type() == SearchBounds.Type.RANGE));
    }

    /**
     * Rule's configuration.
     */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableExposeIndexRule.Config.of()
                .withOperandSupplier(b ->
                                b.operand(IgniteLogicalTableScan.class)
                                        .predicate(ExposeIndexRule::preMatch)
                                        .anyInputs());

        /** {@inheritDoc} */
        @Override default ExposeIndexRule toRule() {
            return new ExposeIndexRule(this);
        }
    }
}
