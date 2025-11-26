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

package org.apache.ignite.internal.client;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.jetbrains.annotations.Nullable;

/**
 * Write context.
 */
public class WriteContext {
    public @Nullable PartitionMapping pm;
    public @Nullable Long enlistmentToken;
    public CompletableFuture<ClientTransaction> firstReqFut;
    public final HybridTimestampTracker tracker;
    public boolean readOnly;
    public @Nullable ClientChannel channel;
    public final int opCode;

    public WriteContext(HybridTimestampTracker tracker, int opCode) {
        this.tracker = tracker;
        this.opCode = opCode;
    }
}
