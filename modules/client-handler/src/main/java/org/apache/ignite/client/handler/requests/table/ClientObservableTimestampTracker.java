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

package org.apache.ignite.client.handler.requests.table;

import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.ObservableTimestampProvider;
import org.jetbrains.annotations.Nullable;

/**
 * Client operation timestamp updater.
 */
public class ClientObservableTimestampTracker implements ObservableTimestampProvider {
    private final HybridTimestamp currentTs;
    private final Consumer<HybridTimestamp> updateTs;

    public ClientObservableTimestampTracker(@Nullable HybridTimestamp currentTs, Consumer<HybridTimestamp> updateTs) {
        this.currentTs = currentTs;
        this.updateTs = updateTs;
    }

    @Override
    public @Nullable HybridTimestamp get() {
        return currentTs;
    }

    @Override
    public void update(@Nullable HybridTimestamp ts) {
        updateTs.accept(ts);
    }
}
