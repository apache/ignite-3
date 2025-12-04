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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningColumns;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningPredicate;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;

/**
 * Partition provider that returns partitions based on the values of provided by execution context at runtime.
 */
public class DynamicPartitionProvider<RowT> implements PartitionProvider<RowT> {

    private final String nodeName;

    private final Int2ObjectMap<NodeWithConsistencyToken> assignments;

    private final PartitionPruningColumns columns;

    private final IgniteTable table;

    /** Constructor. */
    public DynamicPartitionProvider(
            String nodeName,
            Int2ObjectMap<NodeWithConsistencyToken> assignments,
            PartitionPruningColumns columns,
            IgniteTable table
    ) {
        this.nodeName = nodeName;
        this.assignments = assignments;
        this.columns = columns;
        this.table = table;
    }

    /** {@inheritDoc} */
    @Override
    public List<PartitionWithConsistencyToken> getPartitions(ExecutionContext<RowT> ctx) {
        ExpressionFactory expressionFactory = ctx.expressionFactory();

        return PartitionPruningPredicate.prunePartitions(ctx, columns, table, expressionFactory, assignments, nodeName);
    }
}
