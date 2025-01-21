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

package org.apache.ignite.internal.tx.views;

import java.time.Instant;
import java.util.UUID;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxState;
import org.jetbrains.annotations.Nullable;

/**
 * Data class representing transaction.
 */
public class TxInfo {
    private final String id;
    private final @Nullable String state;
    private final Instant startTime;
    private final String type;
    private final String priority;

    static TxInfo readOnly(UUID id) {
        return new TxInfo(id, null, true);
    }

    static TxInfo readWrite(UUID id, TxState txState) {
        return new TxInfo(id, txState, false);
    }

    private TxInfo(UUID id, @Nullable TxState state, boolean readOnly) {
        this.id = id.toString();
        this.state = state == null ? null : state.name();
        this.startTime = Instant.ofEpochMilli(TransactionIds.beginTimestamp(id).getPhysical());
        this.type = readOnly ? TransactionViewDataProvider.READ_ONLY : TransactionViewDataProvider.READ_WRITE;
        this.priority = TransactionIds.priority(id).name();
    }

    public String id() {
        return id;
    }

    public @Nullable String state() {
        return state;
    }

    public Instant startTime() {
        return startTime;
    }

    public String type() {
        return type;
    }

    public String priority() {
        return priority;
    }
}
