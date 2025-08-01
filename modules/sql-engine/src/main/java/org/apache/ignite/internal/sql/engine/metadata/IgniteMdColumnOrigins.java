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

package org.apache.ignite.internal.sql.engine.metadata;

import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ColumnOrigin;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.Nullable;

/**
 * Metadata about the origins of columns.
 */
@SuppressWarnings({"unused", "MethodMayBeStatic"}) // actually all methods are used by runtime generated classes
public class IgniteMdColumnOrigins implements MetadataHandler<ColumnOrigin> {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    new IgniteMdColumnOrigins(), BuiltInMetadata.ColumnOrigin.Handler.class);

    private IgniteMdColumnOrigins() {
    }

    @Override
    public MetadataDef<ColumnOrigin> getDef() {
        return BuiltInMetadata.ColumnOrigin.DEF;
    }

    /** Provides column origin for Subset relation. */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(RelSubset rel,
            RelMetadataQuery mq, int outputColumn) {
        return mq.getColumnOrigins(rel.stripped(), outputColumn);
    }

    /** Provides column origin for IgniteReduceAggregateBase relation. */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(IgniteReduceAggregateBase rel,
            RelMetadataQuery mq, int outputColumn) {
        if (outputColumn < rel.getGroupSet().cardinality()) {
            // get actual index of Group columns.
            return mq.getColumnOrigins(rel.getInput(), rel.getGroupSet().asList().get(outputColumn));
        }

        // TODO: IGNITE-24151 support derived column origin
        return null;
    }

    /** Provides column origin for ProjectableFilterableTableScan relation. */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(ProjectableFilterableTableScan scan,
            RelMetadataQuery mq, int outputColumn) {
        RelOptTable table = scan.getTable();
        List<RexNode> projects = scan.projects();

        if (projects != null) {
            RexNode node = projects.get(outputColumn);
            if (node instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) node;
                outputColumn = inputRef.getIndex();
            } else {
                // TODO: IGNITE-24151 support derived column origin
                return null;
            }
        }

        ImmutableIntList requiredColumns = scan.requiredColumns();
        if (requiredColumns != null) {
            Mapping trimming = Commons.projectedMapping(table.getRowType().getFieldCount(), requiredColumns);

            outputColumn = trimming.getSourceOpt(outputColumn);

            if (outputColumn == -1) {
                return null;
            }
        }

        return Set.of(new RelColumnOrigin(table, outputColumn, false));
    }

    /** Catch all method delegates call to {@link RelMdColumnOrigins#SOURCE} .*/
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(RelNode rel,
            RelMetadataQuery mq, int outputColumn) {
        Method method = ReflectUtil.lookupVisitMethod(
                RelMdColumnOrigins.SOURCE.getClass(),
                rel.getClass(),
                "getColumnOrigins",
                List.of(RelMetadataQuery.class, int.class)
        );

        if (method == null) {
            return null;
        }

        try {
            return (Set<RelColumnOrigin>) method.invoke(RelMdColumnOrigins.SOURCE, rel, mq, outputColumn);
        } catch (IllegalAccessException | InvocationTargetException e) {
            sneakyThrow(e);
        }

        throw new AssertionError("Should not get here");
    }
}
