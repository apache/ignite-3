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

package org.apache.ignite.internal.tx;

import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Counter of read-write transactions that were created and completed locally on the node.
 *
 * <p>Notes for use:</p>
 * <ul>
 *     <li>At begin of the transaction inside the closure of {@link #inUpdateRwTxCountLock(Supplier)}, create {@code beginTs}
 *     and increment the transaction counter using {@link #incrementRwTxCount(HybridTimestamp)}.</li>
 *     <li>At the commit/rollback of the transaction inside the closure of {@link #inUpdateRwTxCountLock(Supplier)}, decrement
 *     the transaction counter using {@link #decrementRwTxCount(HybridTimestamp)}.</li>
 * </ul>
 */
public interface LocalRwTxCounter {
    /**
     * Increases the count of read-write transactions.
     *
     * <p>NOTE: Expected to be executed inside {@link #inUpdateRwTxCountLock(Supplier)} along with creating a {@code beginTs}.</p>
     *
     * @param beginTs Transaction begin timestamp.
     */
    void incrementRwTxCount(HybridTimestamp beginTs);

    /**
     * Decreases the count of read-write transactions.
     *
     * <p>Notes:</p>
     * <ul>
     *     <li>Expected to be executed inside {@link #inUpdateRwTxCountLock(Supplier)}.</li>
     *     <li>May be called multiple times for the same transaction for example due to the transaction recovery procedure must operate in
     *     an independent manner.</li>
     * </ul>
     *
     * @param beginTs Transaction begin timestamp.
     */
    void decrementRwTxCount(HybridTimestamp beginTs);

    /**
     * Method for executing a {@code supplier} on a lock of update the read-write transactions counter.
     *
     * @param supplier Function to run.
     * @param <T> Type of returned value from {@code supplier}.
     * @return Result of the provided function.
     */
    <T> T inUpdateRwTxCountLock(Supplier<T> supplier);
}
