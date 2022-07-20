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

import static java.util.Collections.unmodifiableList;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.lang.IgniteBiTuple;

/** Transaction meta. */
public class TxMeta implements Serializable {
    /** Tx state. */
    private final TxState txState;

    /** The list of enlisted partitions. */
    private final List<IgniteBiTuple<Integer, Integer>> enlistedPartitions;

    /** Commit timestamp. */
    private final Timestamp commitTimestamp;

    /**
     * The constructor.
     * @param txState Tx state.
     * @param enlistedPartitions The list of enlisted partitions.
     * @param commitTimestamp Commit timestamp.
     */
    public TxMeta(TxState txState, List<IgniteBiTuple<Integer, Integer>> enlistedPartitions, Timestamp commitTimestamp) {
        this.txState = txState;
        this.enlistedPartitions = enlistedPartitions;
        this.commitTimestamp = commitTimestamp;
    }

    public TxState txState() {
        return txState;
    }

    public List<IgniteBiTuple<Integer, Integer>> enlistedPartitions() {
        return unmodifiableList(enlistedPartitions);
    }

    public Timestamp commitTimestamp() {
        return commitTimestamp;
    }
}
