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

package org.apache.ignite.internal.tx;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

public interface InternalTransaction extends Transaction {
    /**
     * @return The timestamp.
     */
    Timestamp timestamp();

    /**
     * Returns a transaction map on topology.
     *
     * @return A map of enlisted nodes mapped to a partitions set.
     */
    Map<NetworkAddress, Set<String>> map();

    /**
     * @return The state.
     */
    TxState state();

    /**
     * @param node The node.
     * @param partId Partition group id.
     * @return {@code True} if a node is enlisted into the transaction.
     */
    boolean enlist(NetworkAddress node, String groupId);

    /**
     * Sets a thread of control for a transaction.
     * @param t The thread.
     */
    void thread(Thread t);

    /**
     * @return The thread of control, if presents.
     */
    @Nullable Thread thread();
}
