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

package org.apache.ignite.internal.table.distributed.disaster;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * General interface for disaster recovery requests.
 */
interface DisasterRecoveryRequest {
    /**
     * Returns an ID of the operation, associated with request.
     */
    UUID operationId();

    /**
     * Returns a {@code zoneId}, associated with te request. We can't perform a recovery of several zones in a single request.
     */
    int zoneId();

    /** Returns request type. */
    DisasterRecoveryRequestType type();

    /**
     * The recovery operation itself.
     *
     * @param disasterRecoveryManager Disaster recovery manager.
     * @param revision Revision of the {@link DisasterRecoveryManager#RECOVERY_TRIGGER_KEY} update.
     * @param timestamp Timestamp of {@link DisasterRecoveryManager#RECOVERY_TRIGGER_KEY} update operation.
     * @return New operation future, that completes when operation is completed.
     */
    CompletableFuture<Void> handle(DisasterRecoveryManager disasterRecoveryManager, long revision, HybridTimestamp timestamp);
}
