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

package org.apache.ignite.internal.replicator;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Replication command application result.
 */
public final class CommandApplicationResult {
    private final HybridTimestamp commitTs;
    private final CompletableFuture<?> repFut;

    public CommandApplicationResult(HybridTimestamp commitTs, CompletableFuture<?> repFut) {
        this.commitTs = commitTs;
        this.repFut = repFut;
    }

    public HybridTimestamp getCommitTimestamp() {
        return commitTs;
    }

    public CompletableFuture<?> replicationFuture() {
        return repFut;
    }
}
