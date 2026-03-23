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

package org.apache.ignite.internal.table;

import java.util.Objects;

/**
 * Context for table/index partition scan/lookup operation mostly for SQL need.
 */
public class OperationContext {
    /** Creates a new instance of {@link OperationContext}. */
    public static OperationContext create(TxContext txContext) {
        return new OperationContext(txContext);
    }

    /** Transactional context. */
    private final TxContext txContext;

    private OperationContext(TxContext txContext) {
        this.txContext = Objects.requireNonNull(txContext);
    }

    /**
     * Returns the transactional context.
     *
     * @return The transactional context.
     */
    public TxContext txContext() {
        return txContext;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OperationContext that = (OperationContext) o;
        return Objects.equals(txContext, that.txContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txContext.hashCode());
    }
}
