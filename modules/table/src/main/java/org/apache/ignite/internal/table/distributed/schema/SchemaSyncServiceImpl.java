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

package org.apache.ignite.internal.table.distributed.schema;

import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaSafeTimeTracker;
import org.apache.ignite.internal.schema.SchemaSyncService;

/**
 * A default implementation of {@link SchemaSyncService}.
 */
public class SchemaSyncServiceImpl implements SchemaSyncService {
    private final SchemaSafeTimeTracker schemaSafeTimeTracker;

    private final LongSupplier delayDurationMs;

    private final LongConsumer waitDurationMsRecorder;

    /**
     * Constructor.
     */
    public SchemaSyncServiceImpl(SchemaSafeTimeTracker schemaSafeTimeTracker, LongSupplier delayDurationMs) {
        this(schemaSafeTimeTracker, delayDurationMs, durationMs -> {});
    }

    /**
     * Constructor with metrics recording.
     *
     * @param schemaSafeTimeTracker Schema safe time tracker.
     * @param delayDurationMs Supplier of the delay duration in milliseconds.
     * @param waitDurationMsRecorder Consumer that receives the duration (in ms) of each completed wait.
     */
    public SchemaSyncServiceImpl(
            SchemaSafeTimeTracker schemaSafeTimeTracker,
            LongSupplier delayDurationMs,
            LongConsumer waitDurationMsRecorder
    ) {
        this.schemaSafeTimeTracker = schemaSafeTimeTracker;
        this.delayDurationMs = delayDurationMs;
        this.waitDurationMsRecorder = waitDurationMsRecorder;
    }

    @Override
    public CompletableFuture<Void> waitForMetadataCompleteness(HybridTimestamp ts) {
        long startMs = System.currentTimeMillis();

        return schemaSafeTimeTracker.waitFor(metastoreSafeTimeToWait(ts))
                .whenComplete((v, ex) -> waitDurationMsRecorder.accept(System.currentTimeMillis() - startMs));
    }

    private HybridTimestamp metastoreSafeTimeToWait(HybridTimestamp ts) {
        return ts.subtractPhysicalTime(delayDurationMs.getAsLong());
    }
}
