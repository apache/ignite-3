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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.Nullable;

/**
 *  Provide helper methods for batched DDL commands.
 */
public class DdlBatchingHelper {
    /**
     * Returns {@code true} if node group is compatible with batch group
     * Node: the operation is not commutative.
     */
    public static boolean isCompatible(DdlBatchGroup batchGroup, DdlBatchGroup nodeGroup) {
        return (batchGroup != DdlBatchGroup.OTHER // OTHER group doesn't support batching.
                && batchGroup == nodeGroup) // Groups matched.
                || batchGroup == DdlBatchGroup.DROP;
    }

    /** Returns command kind or {@code null} if command is not batch aware. */
    public static @Nullable DdlBatchGroup extractDdlBatchGroup(SqlNode node) {
        DdlBatchAware batchAwareAnnotation = node.getClass().getDeclaredAnnotation(DdlBatchAware.class);

        return batchAwareAnnotation == null ? null : batchAwareAnnotation.group();
    }

    private DdlBatchingHelper() {
        // No-op.
    }
}
