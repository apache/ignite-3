/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.basic;

import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.OperationType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Invoke closure implementation for a write operation. */
public class SimpleWriteInvokeClosure implements InvokeClosure<Void> {
    /** Data row to write into storage. */
    private final DataRow newRow;

    /**
     * @param newRow Data row to write into the storage.
     */
    public SimpleWriteInvokeClosure(DataRow newRow) {
        this.newRow = newRow;
    }

    /** {@inheritDoc} */
    @Override public void call(@NotNull DataRow row) {
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public DataRow newRow() {
        return newRow;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public OperationType operationType() {
        return OperationType.WRITE;
    }
}
