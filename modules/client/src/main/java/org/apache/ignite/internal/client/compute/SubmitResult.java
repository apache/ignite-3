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

package org.apache.ignite.internal.client.compute;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.PayloadInputChannel;

/**
 * Result of the job submission. Contains unpacked job id and notification future.
 */
class SubmitResult {
    private final UUID jobId;
    private final CompletableFuture<PayloadInputChannel> notificationFuture;

    SubmitResult(UUID jobId, CompletableFuture<PayloadInputChannel> notificationFuture) {
        this.jobId = jobId;
        this.notificationFuture = notificationFuture;
    }

    UUID jobId() {
        return jobId;
    }

    CompletableFuture<PayloadInputChannel> notificationFuture() {
        return notificationFuture;
    }
}
