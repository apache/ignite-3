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

import org.apache.ignite.internal.sql.engine.exec.RowHandler;

/** Resolves assignments according to incoming row, only colocation columns are used. */
public class RowAwareAssignmentResolverImpl<RowT> implements RowAwareAssignmentResolver<RowT> {
    private final RowHandler<RowT> rowHandler;
    private final AssignmentResolverImpl<RowT> resolver;
    private final int[] keys;

    /** Constructor. */
    public RowAwareAssignmentResolverImpl(int partitions, int[] keys, RowHandler<RowT> rowHandler) {
        resolver = new AssignmentResolverImpl<>(partitions, keys.length);

        this.rowHandler = rowHandler;
        this.keys = keys;
    }

    /** {@inheritDoc} */
    @Override
    public int getPartition(RowT row) {
        for (int idx : keys) {
            resolver.append(rowHandler.get(idx, row));
        }

        return resolver.getPartition();
    }
}
